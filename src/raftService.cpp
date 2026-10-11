#include "rafty/raftService.hpp"
#include "rafty/raft.hpp" 

namespace rafty {
    RaftServiceImpl::~RaftServiceImpl() {}

    RaftServiceImpl::RaftServiceImpl(Raft* raft) {
        raft_ = raft;
    }

    grpc::Status RaftServiceImpl::AppendEntries(grpc::ServerContext* context,
                                                const raftpb::AppendEntriesRequest* request,
                                                raftpb::AppendEntriesReply* response) {
        std::vector<ApplyResult> to_apply;

        {
            std::lock_guard<std::mutex> lock(raft_->mtx);
            uint64_t leaderTerm   = request->term();
            uint64_t leaderCommit = request->leadercommit();

            if (leaderTerm < raft_->getCurrentTerm()) {
                response->set_term(raft_->getCurrentTerm());
                response->set_success(false);
                return grpc::Status::OK;
            }

            if (leaderTerm > raft_->getCurrentTerm()) {
                raft_->setCurrentTerm(leaderTerm);
                raft_->setVotedFor(-1);
            }

            if (raft_->getFsmState() != fsmState::FOLLOWER)
                raft_->changeToFollower();

            raft_->resetLastLeaderInteractionTime();

            uint64_t prevLogIndex = request->prevlogindex();
            uint64_t prevLogTerm  = request->prevlogterm();

            if (prevLogIndex >= raft_->log.size()) {
                response->set_term(raft_->getCurrentTerm());
                response->set_success(false);
                return grpc::Status::OK;
            }

            if (raft_->log[prevLogIndex].term != prevLogTerm) {
                response->set_term(raft_->getCurrentTerm());
                response->set_success(false);
                return grpc::Status::OK;
            }

            uint64_t idx_to_add_at = prevLogIndex + 1;
            for (int i = 0; i < request->entries_size(); ++i) {
                if (idx_to_add_at < raft_->log.size() &&
                    raft_->log[idx_to_add_at].term != request->entries(i).term()) {
                    raft_->log.resize(idx_to_add_at);
                }
                if (idx_to_add_at >= raft_->log.size()) {
                    logEntry entry = {request->entries(i).term(),
                                      request->entries(i).command()};
                    raft_->log.push_back(entry);
                }
                idx_to_add_at++;
            }

            if (leaderCommit > raft_->commitIndex)
                raft_->commitIndex = std::min((uint64_t)raft_->log.size() - 1,
                                              leaderCommit);

            // Advance lastApplied and collect entries to apply — still under
            // mtx so no double-apply races with other threads.
            while (raft_->lastApplied < raft_->commitIndex) {
                ++raft_->lastApplied;
                to_apply.push_back({true,
                                    raft_->log[raft_->lastApplied].command,
                                    raft_->lastApplied});
            }

            response->set_term(raft_->getCurrentTerm());
            response->set_success(true);
        } // lock released — safe to call apply()

        // Apply outside the lock so on_apply() never competes with mtx.
        for (auto &r : to_apply) raft_->apply(r);

        return grpc::Status::OK;
    }

    grpc::Status RaftServiceImpl::RequestVote(grpc::ServerContext* context,
                                              const raftpb::RequestVoteRequest* request,
                                              raftpb::RequestVoteResponse* response) {
        std::lock_guard<std::mutex> lock(raft_->mtx);
        uint64_t requesterTerm  = request->term();
        uint64_t candidateId    = request->candidateid();
        uint64_t lastLogIndex   = request->lastlogindex();
        uint64_t lastLogTerm    = request->lastlogterm();
        uint64_t selfLastLogIdx = raft_->log.size() - 1;
        uint64_t selfLastLogTerm = raft_->log[selfLastLogIdx].term;

        if (requesterTerm < raft_->getCurrentTerm()) {
            response->set_term(raft_->getCurrentTerm());
            response->set_votegranted(false);
            return grpc::Status::OK;
        }
        if (requesterTerm > raft_->getCurrentTerm()) {
            raft_->setCurrentTerm(requesterTerm);
            if (raft_->getFsmState() != fsmState::FOLLOWER)
                raft_->changeToFollower();
            raft_->setVotedFor(-1);
        }

        bool grantVote = false;
        if (lastLogTerm > selfLastLogTerm)
            grantVote = true;
        else if (lastLogTerm == selfLastLogTerm && lastLogIndex >= selfLastLogIdx)
            grantVote = true;

        if ((!raft_->hasVotedThisTerm() || raft_->getVotedFor() == candidateId) &&
            grantVote) {
            raft_->setVotedFor(candidateId);
            raft_->resetLastLeaderInteractionTime();
            response->set_term(raft_->getCurrentTerm());
            response->set_votegranted(true);
        } else {
            response->set_term(raft_->getCurrentTerm());
            response->set_votegranted(false);
        }
        return grpc::Status::OK;
    }
}