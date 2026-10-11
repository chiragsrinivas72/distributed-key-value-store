#include <algorithm>
#include <chrono>
#include <cstdint>
#include <format>
#include <memory>
#include <numeric>
#include <string>
#include <unordered_map>
#include <vector>
#include <csignal>

#include "absl/flags/flag.h"
#include "absl/flags/parse.h"

#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/config_gen.hpp"
#include "toolings/test_ctrl.hpp"
#include "kv/kv_client.hpp"

static constexpr uint64_t KV_PORT_OFFSET = 1000;
static constexpr int      NUM_OPS        = 1000;
static constexpr uint64_t NUM_NODES      = 3;
static constexpr uint64_t RAFT_BASE_PORT = 50050;

ABSL_FLAG(int, verbosity, 0, "Verbosity level");

int main(int argc, char **argv) {
    absl::ParseCommandLine(argc, argv);
    rafty::utils::init_logger();

    auto logger_name = "latency";
    auto logger = spdlog::get(logger_name);
    if (!logger) {
        logger = spdlog::basic_logger_mt(
            logger_name, std::format("logs/{}.log", logger_name), true);
    }

    std::vector<rafty::Config> configs;
    std::unordered_map<uint64_t, uint64_t> node_tester_ports;
    uint64_t tester_port = 55001;

    auto insts = toolings::ConfigGen::gen_local_instances(NUM_NODES, RAFT_BASE_PORT);
    std::vector<std::string> kv_addrs;

    for (const auto &inst : insts) {
        std::map<uint64_t, std::string> peer_addrs;
        for (const auto &peer : insts) {
            if (peer.id == inst.id) continue;
            peer_addrs[peer.id] = peer.external_addr;
        }
        rafty::Config config = {
            .id = inst.id, .addr = inst.listening_addr, .peer_addrs = peer_addrs};
        configs.push_back(config);
        node_tester_ports[inst.id] = tester_port++;
        kv_addrs.push_back("localhost:" + std::to_string(inst.port + KV_PORT_OFFSET));
    }

    toolings::DDBConfig ddb_conf{};
    const std::string ctrl_addr = "0.0.0.0:55000";

    auto ctrl = std::make_unique<toolings::RaftTestCtrl>(
        configs, node_tester_ports, "./kv_node",
        ctrl_addr, 0, absl::GetFlag(FLAGS_verbosity), logger, ddb_conf);

    ctrl->register_applier_handler([](testerpb::ApplyResult m) { (void)m; });
    ctrl->run();

    std::this_thread::sleep_for(std::chrono::seconds(2));

    kv::KvClient client(kv_addrs);
    std::vector<double> latencies;
    latencies.reserve(NUM_OPS);

    for (int i = 0; i < NUM_OPS; i++) {
        auto key   = "key_" + std::to_string(i);
        auto value = "value_" + std::to_string(i);

        auto start = std::chrono::steady_clock::now();
        client.put(key, value);
        auto end = std::chrono::steady_clock::now();

        double ms = std::chrono::duration<double, std::milli>(end - start).count();
        latencies.push_back(ms);
    }

    std::sort(latencies.begin(), latencies.end());

    double avg = std::accumulate(latencies.begin(), latencies.end(), 0.0) / latencies.size();
    double p50 = latencies[latencies.size() * 50 / 100];
    double p99 = latencies[latencies.size() * 99 / 100];

    printf("######################################\n");
    printf("#      latAvg       latP50      latP99\n");
    printf("#        (ms)         (ms)        (ms)\n");
    printf("--------------------------------------\n");
    printf("  %10.2f   %10.2f  %10.2f\n", avg, p50, p99);

    ctrl->kill();
    return 0;
}