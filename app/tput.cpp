#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <format>
#include <fstream>
#include <future>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"
#include "kv/kv_client.hpp"

static constexpr uint64_t KV_PORT_OFFSET = 1000;
static constexpr uint64_t NUM_NODES      = 3;
static constexpr uint64_t RAFT_BASE_PORT = 50050;
static constexpr int      OPS_PER_CLIENT = 1000;
static constexpr int      KEYSPACE_SIZE  = 1000;

ABSL_FLAG(int, verbosity, 0, "Verbosity level");

struct RoundResult {
    int    client_count;
    double lat_avg;
    double lat_p50;
    double lat_p90;
    double lat_p99;
    double throughput;
};

std::pair<std::vector<rafty::Config>, std::unordered_map<uint64_t, uint64_t>>
build_configs(std::vector<std::string> &kv_addrs) {
    std::vector<rafty::Config> configs;
    std::unordered_map<uint64_t, uint64_t> node_tester_ports;
    uint64_t tester_port = 55001;

    auto insts = toolings::ConfigGen::gen_local_instances(NUM_NODES, RAFT_BASE_PORT);
    kv_addrs.clear();

    for (const auto &inst : insts) {
        std::map<uint64_t, std::string> peer_addrs;
        for (const auto &peer : insts) {
            if (peer.id == inst.id) continue;
            peer_addrs[peer.id] = peer.external_addr;
        }
        configs.push_back({.id = inst.id, .addr = inst.listening_addr, .peer_addrs = peer_addrs});
        node_tester_ports[inst.id] = tester_port++;
        kv_addrs.push_back("localhost:" + std::to_string(inst.port + KV_PORT_OFFSET));
    }
    return {configs, node_tester_ports};
}

RoundResult run_round(int num_clients, int put_ratio,
                      const std::vector<std::string> &kv_addrs,
                      std::shared_ptr<spdlog::logger> logger) {
    kv::KvClient setup_client(kv_addrs);
    for (int i = 1; i <= KEYSPACE_SIZE; i++) {
        setup_client.put("key_" + std::to_string(i), "init");
    }

    std::vector<std::future<std::vector<double>>> futs;
    auto wall_start = std::chrono::steady_clock::now();

    for (int c = 0; c < num_clients; c++) {
        futs.push_back(std::async(std::launch::async, [&kv_addrs, put_ratio]() {
            kv::KvClient client(kv_addrs);
            std::mt19937 rng(std::random_device{}());
            std::uniform_int_distribution<int> key_dist(1, KEYSPACE_SIZE);
            std::uniform_int_distribution<int> op_dist(1, 100);

            std::vector<double> latencies;
            latencies.reserve(OPS_PER_CLIENT);

            for (int i = 0; i < OPS_PER_CLIENT; i++) {
                auto key = "key_" + std::to_string(key_dist(rng));
                auto start = std::chrono::steady_clock::now();

                if (op_dist(rng) <= put_ratio) {
                    client.put(key, "value");
                } else {
                    client.get(key);
                }

                auto end = std::chrono::steady_clock::now();
                latencies.push_back(
                    std::chrono::duration<double, std::milli>(end - start).count());
            }
            return latencies;
        }));
    }

    std::vector<double> all_latencies;
    for (auto &f : futs) {
        auto lats = f.get();
        all_latencies.insert(all_latencies.end(), lats.begin(), lats.end());
    }

    auto wall_end = std::chrono::steady_clock::now();
    double wall_ms = std::chrono::duration<double, std::milli>(wall_end - wall_start).count();

    std::sort(all_latencies.begin(), all_latencies.end());
    double avg  = std::accumulate(all_latencies.begin(), all_latencies.end(), 0.0) / all_latencies.size();
    double p50  = all_latencies[all_latencies.size() * 50 / 100];
    double p90  = all_latencies[all_latencies.size() * 90 / 100];
    double p99  = all_latencies[all_latencies.size() * 99 / 100];
    double tput = (num_clients * OPS_PER_CLIENT) / (wall_ms / 1000.0);

    return {num_clients, avg, p50, p90, p99, tput};
}

int main(int argc, char **argv) {
    absl::ParseCommandLine(argc, argv);
    rafty::utils::init_logger();

    if (argc < 3) {
        fprintf(stderr, "Usage: ./tput <MaxClientCount> <PutRatio>\n");
        return 1;
    }

    int max_clients = std::stoi(argv[1]);
    int put_ratio   = std::stoi(argv[2]);

    auto logger_name = "tput";
    auto logger = spdlog::get(logger_name);
    if (!logger) {
        logger = spdlog::basic_logger_mt(
            logger_name, std::format("logs/{}.log", logger_name), true);
    }

    std::vector<RoundResult> results;
    toolings::DDBConfig ddb_conf{};
    const std::string ctrl_addr = "0.0.0.0:55000";

    for (int n = 1; n <= max_clients; n *= 2) {
        std::vector<std::string> kv_addrs;
        auto [configs, node_tester_ports] = build_configs(kv_addrs);

        auto ctrl = std::make_unique<toolings::RaftTestCtrl>(
            configs, node_tester_ports, "./kv_node",
            ctrl_addr, 0, absl::GetFlag(FLAGS_verbosity), logger, ddb_conf);

        ctrl->register_applier_handler([](testerpb::ApplyResult m) { (void)m; });
        ctrl->run();

        std::this_thread::sleep_for(std::chrono::seconds(2));

        auto result = run_round(n, put_ratio, kv_addrs, logger);
        results.push_back(result);

        printf("clients=%d avg=%.2f p50=%.2f p90=%.2f p99=%.2f tput=%.1f\n", n, result.lat_avg, result.lat_p50, result.lat_p90, result.lat_p99, result.throughput);

        ctrl->kill();
        std::this_thread::sleep_for(std::chrono::seconds(1));
    }

    std::ofstream out("result.txt");
    out << "##################################################################################\n";
    out << "# clientCount      latAvg       latP50       latP90       latP99        throughput\n";
    out << "#                    (ms)         (ms)         (ms)         (ms)         (ops/sec)\n";
    out << "----------------------------------------------------------------------------------\n";
    for (const auto &r : results) {
        out << std::format("{:>13}  {:>10.2f}   {:>10.2f}   {:>10.2f}   {:>10.2f}   {:>13.0f}\n",
                           r.client_count, r.lat_avg, r.lat_p50,
                           r.lat_p90, r.lat_p99, r.throughput);
    }

    printf("Results written to result.txt\n");
    return 0;
}