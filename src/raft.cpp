#include "common/utils/rand_gen.hpp"
#include "rafty/raft.hpp"
#include "rafty/helper.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

Raft::Raft(const Config &config, MessageQueue<ApplyResult> &ready)
    : logger(utils::logger::get_logger(config.id)), id(config.id),
      listening_addr(config.addr), peer_addrs(config.peer_addrs), dead(false),
      ready_queue(ready), heartbeatInterval(5),
      electionTimeout(randomBetween((uint64_t)150, (uint64_t)300)),
      lastSentHeartbeatTime(std::chrono::steady_clock::now()),
      currentFsmState(fsmState::FOLLOWER), currentTerm(0), service_(this),
      lastInteractionWithLeaderTime(std::chrono::steady_clock::now()),
      votedFor((uint64_t)-1), commitIndex(0), lastApplied(0),
      pendingReplication_(false), is_leader_(false)
{
    logEntry entry = {0, ""};
    log.push_back(entry);
    log.reserve(100000);

    pool_running_ = true;
    for (int i = 0; i < POOL_SIZE; i++)
        pool_threads_.emplace_back(&Raft::threadPoolWorker, this);
}

Raft::~Raft() {
    {
        std::lock_guard<std::mutex> lock(mtx);
        running = false;
    }
    cv.notify_one();
    if (worker.joinable())
        worker.join();

    pool_running_ = false;
    pool_cv_.notify_all();
    for (auto &t : pool_threads_)
        if (t.joinable()) t.join();

    this->stop_server();
}

void Raft::threadPoolWorker() {
    while (pool_running_.load()) {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(pool_mutex_);
            pool_cv_.wait(lock, [this]() {
                return !pool_queue_.empty() || !pool_running_.load();
            });
            if (!pool_running_.load() && pool_queue_.empty()) break;
            task = std::move(pool_queue_.front());
            pool_queue_.pop();
        }
        task();
    }
}

void Raft::submitToPool(std::function<void()> task) {
    {
        std::unique_lock<std::mutex> lock(pool_mutex_);
        pool_queue_.push(std::move(task));
    }
    pool_cv_.notify_one();
}

uint64_t Raft::getCurrentTerm() { return currentTerm; }
uint64_t Raft::getElectionTimeout() { return electionTimeout; }
void Raft::setCurrentTerm(uint64_t newTerm) { currentTerm = newTerm; }

void Raft::changeToFollower() {
    currentFsmState = fsmState::FOLLOWER;
    is_leader_.store(false, std::memory_order_relaxed);
    last_quorum_time_ = {};
    failAllPendingReads();
}

uint64_t Raft::getVotedFor() { return votedFor; }
void Raft::setVotedFor(uint64_t candidateToVoteFor) { votedFor = candidateToVoteFor; }
bool Raft::hasVotedThisTerm() { return votedFor != (uint64_t)-1; }
fsmState Raft::getFsmState() { return currentFsmState; }
uint64_t Raft::getId() { return id; }
void Raft::resetLastLeaderInteractionTime() {
    lastInteractionWithLeaderTime = std::chrono::steady_clock::now();
}

void Raft::sendHeartBeat() {
    struct PeerTask {
        uint64_t peer_id;
        uint64_t current_term;
        uint64_t leader_id;
        uint64_t prevLogIndex;
        uint64_t prevLogTerm;
        std::vector<logEntry> entries;
        uint64_t leaderCommit;
        uint64_t seq_when_sent;
    };

    std::vector<PeerTask> tasks;
    tasks.reserve(peers_.size());

    {
        std::lock_guard<std::mutex> lock(mtx);
        if (currentFsmState != fsmState::LEADER) return;

        uint64_t seq_when_sent = ++heartbeat_seq_;
        heartbeat_requested_for_reads_ = false;

        uint64_t current_term  = currentTerm;
        uint64_t leader_id     = id;
        uint64_t lastLogIndex  = log.size() - 1;
        uint64_t leader_commit = commitIndex;

        for (auto &[peer_id, stub] : peers_) {
            uint64_t nxt          = nextIndex[peer_id];
            uint64_t prevLogIndex = nxt - 1;
            uint64_t prevLogTerm  = log[prevLogIndex].term;

            std::vector<logEntry> entries;
            entries.reserve(lastLogIndex >= nxt ? lastLogIndex - nxt + 1 : 0);
            for (uint64_t i = nxt; i <= lastLogIndex; ++i)
                entries.push_back(log[i]);

            tasks.push_back({peer_id, current_term, leader_id,
                             prevLogIndex, prevLogTerm,
                             std::move(entries), leader_commit, seq_when_sent});
        }
    }

    for (auto &t : tasks) {
        submitToPool([this, t = std::move(t)]() mutable {
            raftpb::AppendEntriesRequest request;
            request.set_term(t.current_term);
            request.set_leaderid(t.leader_id);
            request.set_prevlogindex(t.prevLogIndex);
            request.set_prevlogterm(t.prevLogTerm);
            request.set_leadercommit(t.leaderCommit);

            for (auto &e : t.entries) {
                auto *entry = request.add_entries();
                entry->set_term(e.term);
                entry->set_command(e.command);
            }

            raftpb::AppendEntriesReply response;
            auto context = this->create_context(t.peer_id);
            grpc::Status status =
                peers_[t.peer_id]->AppendEntries(&*context, request, &response);

            if (status.ok()) {
                // Collect entries to apply while holding the lock, then
                // release before calling apply() so on_apply() never runs
                // under mtx.
                std::vector<ApplyResult> to_apply;

                {
                    std::unique_lock<std::mutex> res_lock(mtx);
                    if (response.term() > getCurrentTerm()) {
                        setCurrentTerm(response.term());
                        changeToFollower();
                        setVotedFor(-1);
                        resetLastLeaderInteractionTime();
                        return;
                    }
                    if (currentFsmState != fsmState::LEADER) return;

                    if (response.success()) {
                        uint64_t lastSentIndex = t.prevLogIndex + t.entries.size();
                        matchIndex[t.peer_id] = lastSentIndex;
                        nextIndex[t.peer_id]  = lastSentIndex + 1;

                        resolvePendingReadsAfterHeartbeat(
                            t.peer_id, t.seq_when_sent, response.term());

                        for (uint64_t newCI = log.size() - 1;
                             newCI > commitIndex; --newCI) {
                            if (log[newCI].term != t.current_term) continue;
                            int cnt = 1;
                            int total = static_cast<int>(peers_.size()) + 1;
                            for (auto &[pid, _] : peers_)
                                if (matchIndex[pid] >= newCI) ++cnt;
                            if (cnt > (total / 2)) {
                                commitIndex = newCI;
                                last_quorum_time_ = std::chrono::steady_clock::now();
                                if (!noop_committed_in_term_ &&
                                    noop_index_ > 0 &&
                                    commitIndex >= noop_index_ &&
                                    log[noop_index_].term == t.current_term) {
                                    noop_committed_in_term_ = true;
                                }
                                break;
                            }
                        }

                        // Advance lastApplied and collect entries to apply —
                        // still under mtx so no other thread double-applies.
                        while (lastApplied < commitIndex) {
                            ++lastApplied;
                            to_apply.push_back(
                                {true, log[lastApplied].command, lastApplied});
                        }

                        // Wake work() in case it needs to trigger more
                        // replication (e.g. pending reads).
                        cv.notify_one();
                    } else {
                        if (nextIndex[t.peer_id] > 1) nextIndex[t.peer_id]--;
                    }
                } // res_lock released here — safe to call on_apply

                // Apply outside the lock: calls on_apply() directly via
                // the registered callback with no extra thread hop.
                for (auto &r : to_apply) apply(r);
            }
        });
    }
}

void Raft::resolvePendingReadsAfterHeartbeat(uint64_t peer_id,
                                             uint64_t seq_when_sent,
                                             uint64_t peer_reply_term) {
    if (pending_reads_.empty()) return;
    const int total_nodes = static_cast<int>(peers_.size()) + 1;
    const int majority    = total_nodes / 2 + 1;

    std::vector<std::unique_ptr<ReadIndexRequest>> still_pending;
    still_pending.reserve(pending_reads_.size());

    for (auto &r : pending_reads_) {
        if (peer_reply_term > r->term) {
            r->promise.set_value({0, false});
            continue;
        }
        if (seq_when_sent > r->issued_at_seq) {
            r->confirmed_peers[peer_id] = true;
            int cnt = 1 + static_cast<int>(r->confirmed_peers.size());
            if (cnt >= majority) {
                r->promise.set_value({r->index, true});
                continue;
            }
        }
        still_pending.push_back(std::move(r));
    }
    pending_reads_ = std::move(still_pending);
}

void Raft::failAllPendingReads() {
    for (auto &r : pending_reads_) {
        try { r->promise.set_value({0, false}); } catch (...) {}
    }
    pending_reads_.clear();
    noop_committed_in_term_ = false;
    noop_index_ = 0;
    heartbeat_requested_for_reads_ = false;
}

std::future<Raft::ReadIndexResult> Raft::readIndex() {
    auto req = std::make_unique<ReadIndexRequest>();
    std::future<ReadIndexResult> fut = req->promise.get_future();

    bool trigger_heartbeat = false;
    {
        std::lock_guard<std::mutex> lock(mtx);

        if (currentFsmState != fsmState::LEADER) {
            req->promise.set_value({0, false});
            return fut;
        }
        if (!noop_committed_in_term_) {
            req->promise.set_value({0, false});
            return fut;
        }

        if (isLeaseValid()) {
            req->promise.set_value({commitIndex, true});
            return fut;
        }

        req->term          = currentTerm;
        req->index         = commitIndex;
        req->issued_at_seq = heartbeat_seq_;
        pending_reads_.push_back(std::move(req));

        if (!heartbeat_requested_for_reads_) {
            heartbeat_requested_for_reads_ = true;
            pendingReplication_ = true;
            trigger_heartbeat = true;
        }
    }
    if (trigger_heartbeat)
        cv.notify_one();
    return fut;
}

void Raft::sendLogEntry() { sendHeartBeat(); }

void Raft::startElection() {
    std::lock_guard<std::mutex> lock(mtx);

    auto now = std::chrono::steady_clock::now();
    if (currentFsmState == fsmState::LEADER) return;
    if (now - lastInteractionWithLeaderTime <
        std::chrono::milliseconds(electionTimeout)) return;

    currentFsmState = fsmState::CANDIDATE;
    is_leader_.store(false, std::memory_order_relaxed);
    failAllPendingReads();
    setCurrentTerm(getCurrentTerm() + 1);
    setVotedFor(id);
    electionTimeout = randomBetween((uint64_t)150, (uint64_t)300);
    resetLastLeaderInteractionTime();

    auto votes            = std::make_shared<int>(1);
    uint64_t electionTerm = getCurrentTerm();
    size_t totalNodes     = peers_.size() + 1;
    uint64_t lastLogIndex = log.size() - 1;
    uint64_t lastLogTerm  = log[lastLogIndex].term;

    for (auto &[peer_id, stub] : peers_) {
        submitToPool([this, peer_id, electionTerm, votes, totalNodes,
                      lastLogIndex, lastLogTerm]() {
            raftpb::RequestVoteRequest request;
            request.set_term(electionTerm);
            request.set_candidateid(id);
            request.set_lastlogindex(lastLogIndex);
            request.set_lastlogterm(lastLogTerm);

            raftpb::RequestVoteResponse response;
            auto context = this->create_context(peer_id);
            auto status  = peers_[peer_id]->RequestVote(&*context, request, &response);

            if (status.ok()) {
                std::lock_guard<std::mutex> res_lock(mtx);
                if (getCurrentTerm() != electionTerm ||
                    currentFsmState != fsmState::CANDIDATE) return;
                if (response.term() > getCurrentTerm()) {
                    setCurrentTerm(response.term());
                    changeToFollower();
                    setVotedFor(-1);
                    return;
                }
                if (response.votegranted()) {
                    (*votes)++;
                    if (*votes > (int)(totalNodes / 2)) {
                        currentFsmState = fsmState::LEADER;
                        is_leader_.store(true, std::memory_order_relaxed);
                        last_quorum_time_ = {};
                        for (auto &[pid, _] : peers_) {
                            nextIndex[pid]  = log.size();
                            matchIndex[pid] = 0;
                        }
                        noop_committed_in_term_ = false;
                        logEntry noop = {currentTerm, std::string("NOOP\n")};
                        log.push_back(noop);
                        noop_index_ = log.size() - 1;

                        lastSentHeartbeatTime =
                            std::chrono::steady_clock::time_point{};
                        pendingReplication_ = true;
                        cv.notify_one();
                    }
                }
            }
        });
    }
}

void Raft::work() {
    while (running) {
        auto currentTime = std::chrono::steady_clock::now();
        bool shouldSendHeartbeat = false;
        bool shouldStartElection = false;

        {
            std::unique_lock<std::mutex> lock(mtx);

            auto wakeDeadline = currentTime + std::chrono::milliseconds(heartbeatInterval);
            // Apply is now handled directly by the pool thread that commits
            // the entry, so we no longer wake for lastApplied < commitIndex.
            cv.wait_until(lock, wakeDeadline, [&] {
                return !running || pendingReplication_;
            });

            if (!running) break;
            currentTime = std::chrono::steady_clock::now();

            if (currentFsmState == fsmState::LEADER) {
                bool intervalElapsed = currentTime - lastSentHeartbeatTime >=
                    std::chrono::milliseconds(heartbeatInterval);
                if (intervalElapsed || pendingReplication_) {
                    shouldSendHeartbeat   = true;
                    lastSentHeartbeatTime = currentTime;
                    pendingReplication_   = false;
                }
            } else {
                if (currentTime - lastInteractionWithLeaderTime >=
                    std::chrono::milliseconds(electionTimeout))
                    shouldStartElection = true;
            }
        }

        if (shouldSendHeartbeat) sendHeartBeat();
        if (shouldStartElection) startElection();
    }
}

void Raft::run() {
    running = true;
    worker  = std::thread(&Raft::work, this);
}

State Raft::get_state() const {
    std::lock_guard<std::mutex> lock(const_cast<std::mutex &>(mtx));
    return {currentTerm, currentFsmState == fsmState::LEADER};
}

ProposalResult Raft::propose(const std::string &data) {
    uint64_t index, term;
    {
        std::lock_guard<std::mutex> lock(mtx);
        if (currentFsmState != fsmState::LEADER)
            return {0, currentTerm, false};
        logEntry entry = {currentTerm, data};
        log.push_back(entry);
        pendingReplication_ = true;
        cv.notify_one();
        index = log.size() - 1;
        term  = currentTerm;
    }
    return {index, term, true};
}

ProposalResult Raft::propose_sync(const std::string &data) {
    // TODO: lab 3
}

} // namespace rafty