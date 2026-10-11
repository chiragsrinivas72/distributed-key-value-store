#pragma once

#include <cstdint>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <string>
#include <unordered_map>
#include <vector>

#include <grpcpp/grpcpp.h>

#include "common/common.hpp"
#include "common/config.hpp"
#include "common/logger.hpp"
#include "toolings/msg_queue.hpp"
#include "rafty/fsmState.hpp"
#include "rafty/raftService.hpp"

#include "raft.grpc.pb.h"

using namespace toolings;

namespace rafty {
using RaftServiceStub = std::unique_ptr<raftpb::RaftService::Stub>;
using grpc::Server;

struct logEntry {
  uint64_t term;
  std::string command;
};

class Raft {
public:
  Raft(const Config &config, MessageQueue<ApplyResult> &ready);
  ~Raft();

  void run();
  ProposalResult propose(const std::string &data);
  State get_state() const;
  uint64_t getElectionTimeout();
  fsmState getFsmState();
  bool isLeader() const { return is_leader_.load(std::memory_order_relaxed); }

  ProposalResult propose_sync(const std::string &data);

  // Register a callback invoked directly on the committing thread each time
  // an entry is ready to apply. When set, ready_queue is bypassed entirely —
  // eliminating two OS thread wakeups and the unbuffered-queue rendezvous
  // from the write critical path. Must be called before run().
  void setApplyCallback(std::function<void(const ApplyResult &)> cb) {
    apply_cb_ = std::move(cb);
  }

  struct ReadIndexResult { uint64_t index; bool ok; };
  std::future<ReadIndexResult> readIndex();

  uint64_t getLastApplied() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex&>(mtx));
    return lastApplied;
  }

  void start_server();
  void stop_server();
  void connect_peers();
  bool is_dead() const;
  void kill();
  void handleRequestVoteRPC(const raftpb::RequestVoteRequest* request, raftpb::RequestVoteResponse* response);
  void handleAppendEntriesRPC(const raftpb::AppendEntriesRequest* request, raftpb::AppendEntriesReply* response);

private:
  std::unique_ptr<grpc::ClientContext> create_context(uint64_t to) const;
  void apply(const ApplyResult &result);
  void work();
  void resetLastLeaderInteractionTime();
  uint64_t getCurrentTerm();
  void setCurrentTerm(uint64_t newTerm);
  void changeToFollower();
  uint64_t getVotedFor();
  uint64_t getId();
  void setVotedFor(uint64_t candidateToVoteFor);
  bool hasVotedThisTerm();

  void sendHeartBeat();
  void sendLogEntry();
  void startElection();

  void threadPoolWorker();
  void submitToPool(std::function<void()> task);

  bool isLeaseValid() const {
    if (!noop_committed_in_term_) return false;
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now() - last_quorum_time_
    ).count();
    return elapsed < static_cast<int64_t>(
        kMinElectionTimeoutMs - kLeaseBufferMs - heartbeatInterval);
  }

protected:
  mutable std::mutex mtx;
  std::unique_ptr<rafty::utils::logger> logger;

private:
  uint64_t id;
  std::string listening_addr;
  std::map<uint64_t, std::string> peer_addrs;

  std::atomic<bool> dead;
  MessageQueue<ApplyResult> &ready_queue;

  // Direct apply callback. When non-null, apply() calls this instead of
  // enqueuing to ready_queue.
  std::function<void(const ApplyResult &)> apply_cb_;

  std::unordered_map<uint64_t, RaftServiceStub> peers_;
  std::unique_ptr<Server> server_;

  friend class RaftServiceImpl;
  RaftServiceImpl service_;
  uint64_t currentTerm;
  fsmState currentFsmState;

  uint64_t heartbeatInterval;
  uint64_t electionTimeout;
  std::chrono::steady_clock::time_point lastSentHeartbeatTime;
  std::chrono::steady_clock::time_point lastInteractionWithLeaderTime;

  uint64_t votedFor;

  std::thread worker;
  std::atomic<bool> running{false};
  std::condition_variable cv;

  std::vector<logEntry> log;

  uint64_t commitIndex;
  uint64_t lastApplied;

  std::unordered_map<uint64_t, uint64_t> nextIndex;
  std::unordered_map<uint64_t, uint64_t> matchIndex;

  bool pendingReplication_ = false;
  std::atomic<bool> is_leader_{false};

  static constexpr int POOL_SIZE = 8;
  std::vector<std::thread> pool_threads_;
  std::queue<std::function<void()>> pool_queue_;
  std::mutex pool_mutex_;
  std::condition_variable pool_cv_;
  std::atomic<bool> pool_running_{false};

  struct ReadIndexRequest {
    uint64_t term;
    uint64_t index;
    uint64_t issued_at_seq;
    std::unordered_map<uint64_t, bool> confirmed_peers;
    std::promise<ReadIndexResult> promise;
  };

  uint64_t heartbeat_seq_ = 0;
  std::vector<std::unique_ptr<ReadIndexRequest>> pending_reads_;

  bool heartbeat_requested_for_reads_ = false;
  bool noop_committed_in_term_ = false;
  uint64_t noop_index_ = 0;

  std::chrono::steady_clock::time_point last_quorum_time_{};

  static constexpr uint64_t kMinElectionTimeoutMs = 150;
  static constexpr uint64_t kLeaseBufferMs        = 20;

  void resolvePendingReadsAfterHeartbeat(uint64_t peer_id,
                                         uint64_t seq_when_sent,
                                         uint64_t peer_reply_term);
  void failAllPendingReads();
};
} // namespace rafty

#include "rafty/impl/raft.ipp"