#ifndef ZEPHYR_GRPC_THREAD_POOL_H_
#define ZEPHYR_GRPC_THREAD_POOL_H_

#include "grpcpp/grpcpp.h"
#include "zephyr/utils/imports.h"

namespace zephyr {
namespace grpc {

class GrpcCQTag {
public:
  virtual void OnCompleted(bool ok) = 0;
};

class GrpcThreadPool {
public:
  static GrpcThreadPool *GetInstance() {
    static GrpcThreadPool grpc_thread_pool(thread::hardware_concurrency());
    return &grpc_thread_pool;
  }

  GrpcThreadPool(GrpcThreadPool const &) = delete;
  void operator=(GrpcThreadPool const &) = delete;

  ::grpc::CompletionQueue *NextCompletionQueue() {
    lock_guard<mutex> lock(mu_);
    return threads_[next_round_robin_assignment_++ % threads_.size()]
        .completion_queue();
  }

private:
  explicit GrpcThreadPool(size_t thread_count)
      : threads_(thread_count), next_round_robin_assignment_(0) {}

  class GrpcThread {
  public:
    GrpcThread() : thread_(&GrpcThread::CompleteGrpcCall, this) {}

    void CompleteGrpcCall() {
      void *tag;
      bool ok = false;

      while (completion_queue_.Next(&tag, &ok)) {
        auto *cq_tag = static_cast<GrpcCQTag *>(tag);
        cq_tag->OnCompleted(ok);
      }
    }

    ~GrpcThread() {
      completion_queue_.Shutdown();
      thread_.join();
    }

    ::grpc::CompletionQueue *completion_queue() { return &completion_queue_; }

  private:
    ::grpc::CompletionQueue completion_queue_;
    thread thread_;
  };

  vector<GrpcThread> threads_;
  size_t next_round_robin_assignment_;
  mutex mu_;
};

} // namespace grpc
} // namespace zephyr

#endif // ZEPHYR_GRPC_THREAD_POOL_H_
