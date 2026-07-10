#include "benchmark_sim/periodic_metrics_collector.h"

#include <stdexcept>
#include <utility>

namespace bsim {

PeriodicMetricsCollector::PeriodicMetricsCollector(
    std::uint32_t sample_count, std::chrono::milliseconds interval,
    SampleHandler sample_handler)
    : sample_count_(sample_count),
      interval_(interval),
      sample_handler_(std::move(sample_handler)) {
  if (interval_.count() <= 0) {
    throw std::runtime_error("metrics collection interval must be positive");
  }
  if (!sample_handler_) {
    throw std::runtime_error("metrics collection requires a sample handler");
  }
}

PeriodicMetricsCollector::~PeriodicMetricsCollector() { Stop(); }

void PeriodicMetricsCollector::Start() {
  if (started_) {
    throw std::runtime_error("periodic metrics collector is already started");
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_requested_ = false;
  }
  failure_ = nullptr;
  started_ = true;
  thread_ = std::thread(&PeriodicMetricsCollector::Run, this);
}

void PeriodicMetricsCollector::Wait() {
  if (!started_) {
    return;
  }
  if (thread_.joinable()) {
    thread_.join();
  }
  started_ = false;
  if (failure_) {
    std::rethrow_exception(failure_);
  }
}

void PeriodicMetricsCollector::Stop() {
  if (!started_) {
    return;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    stop_requested_ = true;
  }
  condition_.notify_all();
  if (thread_.joinable()) {
    thread_.join();
  }
  started_ = false;
}

bool PeriodicMetricsCollector::StopRequested() {
  std::lock_guard<std::mutex> lock(mutex_);
  return stop_requested_;
}

void PeriodicMetricsCollector::Run() {
  auto next_sample = std::chrono::steady_clock::now() + interval_;
  for (std::uint32_t index = 0; index < sample_count_; ++index) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (condition_.wait_until(lock, next_sample,
                              [this] { return stop_requested_; })) {
      return;
    }
    lock.unlock();
    try {
      sample_handler_(index + 1U);
    } catch (...) {
      failure_ = std::current_exception();
      return;
    }
    next_sample += interval_;
  }
}

}  // namespace bsim
