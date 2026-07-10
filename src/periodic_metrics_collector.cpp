#include "bbp/periodic_metrics_collector.h"

#include <algorithm>
#include <stdexcept>
#include <utility>

namespace bbp {

PeriodicMetricsCollector::PeriodicMetricsCollector(
    std::uint32_t sample_count, std::chrono::milliseconds interval,
    SampleHandler sample_handler, ExternalStopRequested external_stop_requested)
    : sample_count_(sample_count),
      interval_(interval),
      sample_handler_(std::move(sample_handler)),
      external_stop_requested_(std::move(external_stop_requested)) {
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
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (stop_requested_) {
      return true;
    }
  }
  return external_stop_requested_ && external_stop_requested_();
}

void PeriodicMetricsCollector::Run() {
  constexpr auto kExternalStopPollInterval = std::chrono::milliseconds(50);
  auto next_sample = std::chrono::steady_clock::now() + interval_;
  std::uint32_t index = 0U;
  while (sample_count_ == 0U || index < sample_count_) {
    std::unique_lock<std::mutex> lock(mutex_);
    const auto wake_time =
        external_stop_requested_
            ? std::min(next_sample, std::chrono::steady_clock::now() +
                                        kExternalStopPollInterval)
            : next_sample;
    if (condition_.wait_until(lock, wake_time,
                              [this] { return stop_requested_; })) {
      return;
    }
    lock.unlock();
    if (external_stop_requested_ && external_stop_requested_()) {
      return;
    }
    if (std::chrono::steady_clock::now() < next_sample) {
      continue;
    }
    try {
      sample_handler_(index + 1U);
    } catch (...) {
      failure_ = std::current_exception();
      return;
    }
    ++index;
    next_sample += interval_;
  }
}

}  // namespace bbp
