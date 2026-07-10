#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <mutex>
#include <thread>

namespace bsim {

class PeriodicMetricsCollector {
 public:
  using SampleHandler = std::function<void(std::uint32_t)>;

  PeriodicMetricsCollector(std::uint32_t sample_count,
                           std::chrono::milliseconds interval,
                           SampleHandler sample_handler);
  PeriodicMetricsCollector(const PeriodicMetricsCollector&) = delete;
  PeriodicMetricsCollector& operator=(const PeriodicMetricsCollector&) = delete;
  ~PeriodicMetricsCollector();

  void Start();
  void Wait();
  void Stop();
  bool StopRequested();

 private:
  void Run();

  std::uint32_t sample_count_;
  std::chrono::milliseconds interval_;
  SampleHandler sample_handler_;
  std::mutex mutex_;
  std::condition_variable condition_;
  std::thread thread_;
  std::exception_ptr failure_;
  bool stop_requested_ = false;
  bool started_ = false;
};

}  // namespace bsim
