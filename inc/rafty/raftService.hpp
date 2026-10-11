#ifndef RAFT_SERVICE_H
#define RAFT_SERVICE_H

#include <grpcpp/grpcpp.h>
#include "raft.grpc.pb.h"

namespace rafty {
    class Raft;
    class RaftServiceImpl final : public raftpb::RaftService::Service {
        public:
            RaftServiceImpl() = default;
            explicit RaftServiceImpl(Raft* raft);
            ~RaftServiceImpl() override;
            grpc::Status AppendEntries(grpc::ServerContext* context, const raftpb::AppendEntriesRequest* request, raftpb::AppendEntriesReply* response) override;
            grpc::Status RequestVote(grpc::ServerContext* context, const raftpb::RequestVoteRequest* request, raftpb::RequestVoteResponse* response) override;
        private:
            Raft* raft_;
    };
} // rafty


#endif //RAFT_SERVICE_H