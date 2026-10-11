#pragma once

#include <grpcpp/health_check_service_interface.h>
#include <grpcpp/ext/proto_server_reflection_plugin.h>
#include "common/utils/net_intercepter.hpp"
#ifdef TRACING
#include "common/utils/tracing.hpp"
#endif
#include "rafty/raft.hpp"
#include "rafty/raftService.hpp"

namespace rafty {
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::experimental::ClientInterceptorFactoryInterface;
using grpc::experimental::CreateCustomChannelWithInterceptors;

inline void Raft::start_server() {
  grpc::EnableDefaultHealthCheckService(false);
  grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  
  ServerBuilder builder;
  builder.AddListeningPort(this->listening_addr,
                           grpc::InsecureServerCredentials());

#ifdef TRACING
  builder.experimental().SetInterceptorCreators(
      tracing::CreateServerTracingInterceptors());
#endif

  builder.RegisterService(&service_);

  std::unique_ptr<Server> server(builder.BuildAndStart());
  logger->info("Raft server {} listening on {}", id, listening_addr);

  this->server_ = std::move(server);

  std::thread([this] { this->server_->Wait(); }).detach();
}

inline void Raft::stop_server() {
  if (this->server_) {
    this->server_->Shutdown();
  }
}

inline void Raft::connect_peers() {
  grpc::ChannelArguments args;
  args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 200);
  args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 50);
  args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 50);

  for (const auto &peer_addr : peer_addrs) {
    logger->info("Connecting to peer {} at {}", peer_addr.first,
                 peer_addr.second);
    std::vector<std::unique_ptr<ClientInterceptorFactoryInterface>>
        interceptor_creators;
    interceptor_creators.push_back(
        std::make_unique<ByteCountingInterceptorFactory>());
    interceptor_creators.push_back(std::make_unique<NetInterceptorFactory>());
#ifdef TRACING
    interceptor_creators.push_back(std::make_unique<tracing::TracingClientInterceptorFactory>());
#endif
    auto channel = CreateCustomChannelWithInterceptors(
        peer_addr.second, grpc::InsecureChannelCredentials(), args,
        std::move(interceptor_creators));
    auto stub = raftpb::RaftService::NewStub(std::move(channel));
    peers_[peer_addr.first] = std::move(stub);
  }
}

inline bool Raft::is_dead() const { return this->dead.load(); }

inline void Raft::kill() {
  this->dead.store(true);
}

inline std::unique_ptr<grpc::ClientContext>
Raft::create_context(uint64_t to) const {
  std::unique_ptr<grpc::ClientContext> context =
      std::make_unique<grpc::ClientContext>();
  context->AddMetadata("from", std::to_string(this->id));
  context->AddMetadata("to", std::to_string(to));
  return context;
}

// Call the registered callback if one exists; otherwise fall back to the
// original unbuffered ready_queue handoff.
inline void Raft::apply(const ApplyResult &result) {
  if (apply_cb_) {
    apply_cb_(result);
  } else {
    this->ready_queue.enqueue(result);
  }
}

} // namespace rafty