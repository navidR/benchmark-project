#pragma once

#include <signal.h>

#include <atomic>
#include <stop_token>
#include <thread>

namespace bbp {

class SignalStopMonitor {
 public:
  SignalStopMonitor();
  SignalStopMonitor(const SignalStopMonitor&) = delete;
  SignalStopMonitor& operator=(const SignalStopMonitor&) = delete;
  ~SignalStopMonitor();

  [[nodiscard]] std::stop_token GetToken() const noexcept;
  [[nodiscard]] int ReceivedSignal() const noexcept;

 private:
  void Run(std::stop_token stop_token) noexcept;

  sigset_t signals_{};
  sigset_t previous_mask_{};
  std::stop_source stop_source_;
  std::jthread worker_;
  std::atomic<int> received_signal_ = 0;
  int signal_fd_ = -1;
  bool mask_installed_ = false;
};

}  // namespace bbp
