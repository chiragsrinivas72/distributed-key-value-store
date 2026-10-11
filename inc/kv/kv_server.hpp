#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "kv.grpc.pb.h"
#include "rafty/raft.hpp"

namespace kv {

struct ClientRequestCache {
  uint64_t seq_num;
  kvpb::KvStatus status;
};

class KvServer : public kvpb::KvService::Service {
public:
  explicit KvServer(rafty::Raft &raft) : raft_(raft), running_(true) {
    raft_.setApplyCallback(
        [this](const rafty::ApplyResult &r) { on_apply(r); });
    batch_thread_ = std::thread(&KvServer::batchWorker, this);
  }

  ~KvServer() {
    running_.store(false);
    raft_.setApplyCallback(nullptr);
    batch_cv_.notify_all();
    apply_cv_.notify_all();
    if (batch_thread_.joinable())
      batch_thread_.join();
  }

  void on_apply(const rafty::ApplyResult &result) {
    std::vector<std::shared_ptr<std::promise<kvpb::KvStatus>>> to_notify;

    {
      // rifl_mutex_ (exclusive) guards rifl_map_ updates.
      // store_mutex_ (exclusive) guards store_ writes.
      // We take rifl first, then store — consistent order everywhere.
      std::unique_lock<std::shared_mutex> rlock(rifl_mutex_);

      const std::string &data = result.data;
      size_t pos = 0;

      auto nextLine = [&]() -> std::string_view {
        size_t end = data.find('\n', pos);
        if (end == std::string::npos) end = data.size();
        std::string_view sv(data.data() + pos, end - pos);
        pos = end + 1;
        return sv;
      };

      // Collect mutations first so we hold store_mutex_ for the
      // shortest possible window.
      struct Mutation {
        std::string_view op_type;
        std::string key;
        std::string value;
        uint64_t client_id;
        uint64_t seq_num;
      };
      std::vector<Mutation> mutations;

      auto parseOne = [&](std::string_view op_type) {
        if (op_type == "NOOP") return;
        auto key_sv = nextLine();
        std::string value;
        if (op_type == "PUT" || op_type == "APPEND")
          value = std::string(nextLine());
        auto cid_sv = nextLine();
        auto seq_sv = nextLine();
        if (cid_sv.empty() || seq_sv.empty()) return;

        uint64_t client_id = std::stoull(std::string(cid_sv));
        uint64_t seq_num   = std::stoull(std::string(seq_sv));

        auto it = rifl_map_.find(client_id);
        if (it != rifl_map_.end() && seq_num <= it->second.seq_num) return;

        rifl_map_[client_id] = {seq_num, kvpb::KV_SUCCESS};
        mutations.push_back({op_type, std::string(key_sv), std::move(value),
                             client_id, seq_num});
      };

      auto header = nextLine();
      if (header == "BATCH") {
        int n = std::stoi(std::string(nextLine()));
        for (int i = 0; i < n; ++i)
          parseOne(nextLine());
      } else {
        parseOne(header);
      }

      // Apply mutations to store under store_mutex_ (exclusive write lock).
      if (!mutations.empty()) {
        std::unique_lock<std::shared_mutex> wlock(store_mutex_);
        for (auto &m : mutations) {
          if (m.op_type == "PUT")
            store_[m.key] = std::move(m.value);
          else if (m.op_type == "APPEND")
            store_[m.key] += m.value;
        }
      }
    } // rifl_mutex_ released

    // Update last_index_applied_ atomically so Get's fast path can avoid
    // taking apply_mutex_ entirely when the index is already satisfied.
    last_index_applied_.store(result.index, std::memory_order_release);
    apply_cv_.notify_all();

    // Resolve waiting promises under apply_mutex_.
    {
      std::unique_lock<std::mutex> alock(apply_mutex_);
      auto it = index_promises_.find(result.index);
      if (it != index_promises_.end()) {
        to_notify = std::move(it->second);
        index_promises_.erase(it);
      }
    }

    for (auto &p : to_notify)
      p->set_value(kvpb::KV_SUCCESS);
  }

  grpc::Status Put(grpc::ServerContext *,
                   const kvpb::PutRequest *request,
                   kvpb::KvResponse *response) override {
    if (!raft_.isLeader()) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    {
      std::shared_lock<std::shared_mutex> lock(rifl_mutex_);
      auto it = rifl_map_.find(request->client_id());
      if (it != rifl_map_.end() && it->second.seq_num >= request->seq_num()) {
        response->set_status(it->second.status);
        return grpc::Status::OK;
      }
    }

    std::string op = "PUT\n" + request->key() + "\n" + request->value() + "\n" +
                     std::to_string(request->client_id()) + "\n" +
                     std::to_string(request->seq_num()) + "\n";

    response->set_status(submitOp(std::move(op)));
    return grpc::Status::OK;
  }

  grpc::Status Get(grpc::ServerContext *,
                   const kvpb::GetRequest *request,
                   kvpb::GetResponse *response) override {
    if (!raft_.isLeader()) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    auto fut = raft_.readIndex();
    auto timeout = std::chrono::milliseconds(raft_.getElectionTimeout() * 3);
    if (fut.wait_for(timeout) == std::future_status::timeout) {
      response->set_status(kvpb::KV_TIMEOUT);
      return grpc::Status::OK;
    }
    auto result = fut.get();
    if (!result.ok) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    // Fast path: if the apply thread has already passed this index
    // (common under lease reads), skip apply_mutex_ entirely.
    if (last_index_applied_.load(std::memory_order_acquire) < result.index) {
      std::unique_lock<std::mutex> lock(apply_mutex_);
      auto deadline = std::chrono::steady_clock::now() + timeout;
      if (!apply_cv_.wait_until(lock, deadline, [&] {
            return last_index_applied_.load(std::memory_order_acquire)
                   >= result.index;
          })) {
        response->set_status(kvpb::KV_TIMEOUT);
        return grpc::Status::OK;
      }
    }

    // Shared (read) lock — concurrent Gets run in parallel.
    {
      std::shared_lock<std::shared_mutex> slock(store_mutex_);
      auto kit = store_.find(request->key());
      response->set_value(kit != store_.end() ? kit->second : "");
      response->set_status(kvpb::KV_SUCCESS);
    }
    return grpc::Status::OK;
  }

  grpc::Status Append(grpc::ServerContext *,
                      const kvpb::AppendRequest *request,
                      kvpb::KvResponse *response) override {
    if (!raft_.isLeader()) {
      response->set_status(kvpb::KV_NOTLEADER);
      return grpc::Status::OK;
    }

    {
      std::shared_lock<std::shared_mutex> lock(rifl_mutex_);
      auto it = rifl_map_.find(request->client_id());
      if (it != rifl_map_.end() && it->second.seq_num >= request->seq_num()) {
        response->set_status(it->second.status);
        return grpc::Status::OK;
      }
    }

    std::string op = "APPEND\n" + request->key() + "\n" + request->value() + "\n" +
                     std::to_string(request->client_id()) + "\n" +
                     std::to_string(request->seq_num()) + "\n";

    response->set_status(submitOp(std::move(op)));
    return grpc::Status::OK;
  }

private:
  struct QueueItem {
    std::string serialized;
    std::shared_ptr<std::promise<kvpb::KvStatus>> promise;
  };

  kvpb::KvStatus submitOp(std::string &&serialized_op) {
    auto prom = std::make_shared<std::promise<kvpb::KvStatus>>();
    auto fut  = prom->get_future();

    bool i_am_proposer = false;
    QueueItem self_item;

    {
      std::unique_lock<std::mutex> lock(batch_mutex_);
      if (!proposing_) {
        proposing_ = true;
        i_am_proposer = true;
        self_item = {std::move(serialized_op), prom};
      } else {
        pending_ops_.push({std::move(serialized_op), prom});
        batch_cv_.notify_one();
      }
    }

    if (i_am_proposer)
      proposeAndRegister(std::move(self_item));

    auto timeout = std::chrono::milliseconds(raft_.getElectionTimeout() * 3);
    if (fut.wait_for(timeout) == std::future_status::timeout)
      return kvpb::KV_TIMEOUT;
    return fut.get();
  }

  void proposeAndRegister(QueueItem &&own_item) {
    std::vector<QueueItem> batch;
    batch.push_back(std::move(own_item));

    {
      std::unique_lock<std::mutex> lock(batch_mutex_);
      constexpr size_t kMaxBatchSize = 512;
      while (!pending_ops_.empty() && batch.size() < kMaxBatchSize) {
        batch.push_back(std::move(pending_ops_.front()));
        pending_ops_.pop();
      }
    }

    doProposeBatch(batch);

    {
      std::unique_lock<std::mutex> lock(batch_mutex_);
      proposing_ = false;
      if (!pending_ops_.empty())
        batch_cv_.notify_one();
    }
  }

  void doProposeBatch(std::vector<QueueItem> &batch) {
    if (batch.empty()) return;

    if (!raft_.isLeader()) {
      for (auto &item : batch)
        item.promise->set_value(kvpb::KV_NOTLEADER);
      return;
    }

    std::string payload;
    if (batch.size() == 1) {
      payload = std::move(batch[0].serialized);
    } else {
      size_t total = 32;
      for (auto &item : batch) total += item.serialized.size();
      payload.reserve(total);
      payload = "BATCH\n" + std::to_string(batch.size()) + "\n";
      for (auto &item : batch)
        payload += item.serialized;
    }

    rafty::ProposalResult result = raft_.propose(payload);

    if (!result.is_leader) {
      for (auto &item : batch)
        item.promise->set_value(kvpb::KV_NOTLEADER);
      return;
    }

    {
      std::unique_lock<std::mutex> lock(apply_mutex_);
      auto &promises = index_promises_[result.index];
      for (auto &item : batch)
        promises.push_back(item.promise);
    }
  }

  void batchWorker() {
    while (running_.load()) {
      std::vector<QueueItem> batch;

      {
        std::unique_lock<std::mutex> lock(batch_mutex_);
        batch_cv_.wait(lock, [this]() {
          return (!pending_ops_.empty() && !proposing_) || !running_.load();
        });
        if (!running_.load() && pending_ops_.empty()) break;
        if (proposing_) continue;

        constexpr size_t kMaxBatchSize = 512;
        while (!pending_ops_.empty() && batch.size() < kMaxBatchSize) {
          batch.push_back(std::move(pending_ops_.front()));
          pending_ops_.pop();
        }
        if (batch.empty()) continue;
        proposing_ = true;
      }

      doProposeBatch(batch);

      {
        std::unique_lock<std::mutex> lock(batch_mutex_);
        proposing_ = false;
        if (!pending_ops_.empty())
          batch_cv_.notify_one();
      }
    }
  }

  rafty::Raft &raft_;

  // store_ is protected by store_mutex_ (shared_mutex) so concurrent Gets
  // can read in parallel. on_apply takes an exclusive write lock.
  std::unordered_map<std::string, std::string> store_;
  mutable std::shared_mutex store_mutex_;

  // rifl_map_ is protected by rifl_mutex_. on_apply takes exclusive write
  // lock; Put/Append RIFL checks take shared read lock.
  std::unordered_map<uint64_t, ClientRequestCache> rifl_map_;
  std::shared_mutex rifl_mutex_;

  // last_index_applied_ is atomic so Get's fast path can check it without
  // taking apply_mutex_ when the index is already satisfied.
  std::atomic<uint64_t> last_index_applied_{0};

  // apply_mutex_ now only guards index_promises_ and apply_cv_ waits.
  std::mutex apply_mutex_;
  std::condition_variable apply_cv_;

  std::unordered_map<uint64_t,
    std::vector<std::shared_ptr<std::promise<kvpb::KvStatus>>>>
    index_promises_;

  std::queue<QueueItem> pending_ops_;
  std::mutex batch_mutex_;
  std::condition_variable batch_cv_;
  bool proposing_ = false;
  std::thread batch_thread_;
  std::atomic<bool> running_{false};
};

} // namespace kv