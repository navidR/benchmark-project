#include "bbp/signal_stop_monitor.h"

#include <poll.h>
#include <pthread.h>
#include <sys/signalfd.h>
#include <unistd.h>

#include <cerrno>
#include <cstdint>
#include <cstring>
#include <stdexcept>
#include <string>

namespace bbp {

SignalStopMonitor::SignalStopMonitor() {
  sigemptyset(&signals_);
  sigaddset(&signals_, SIGINT);
  sigaddset(&signals_, SIGTERM);

  const int mask_status =
      pthread_sigmask(SIG_BLOCK, &signals_, &previous_mask_);
  if (mask_status != 0) {
    throw std::runtime_error("block termination signals failed: " +
                             std::string(std::strerror(mask_status)));
  }
  mask_installed_ = true;

  signal_fd_ = signalfd(-1, &signals_, SFD_CLOEXEC | SFD_NONBLOCK);
  if (signal_fd_ < 0) {
    const int saved_errno = errno;
    static_cast<void>(pthread_sigmask(SIG_SETMASK, &previous_mask_, nullptr));
    mask_installed_ = false;
    throw std::runtime_error("signalfd failed: " +
                             std::string(std::strerror(saved_errno)));
  }

  try {
    worker_ =
        std::jthread([this](std::stop_token stop_token) { Run(stop_token); });
  } catch (...) {
    close(signal_fd_);
    signal_fd_ = -1;
    static_cast<void>(pthread_sigmask(SIG_SETMASK, &previous_mask_, nullptr));
    mask_installed_ = false;
    throw;
  }
}

SignalStopMonitor::~SignalStopMonitor() {
  worker_.request_stop();
  if (worker_.joinable()) {
    worker_.join();
  }
  if (signal_fd_ >= 0) {
    close(signal_fd_);
    signal_fd_ = -1;
  }
  if (mask_installed_) {
    static_cast<void>(pthread_sigmask(SIG_SETMASK, &previous_mask_, nullptr));
  }
}

std::stop_token SignalStopMonitor::GetToken() const noexcept {
  return stop_source_.get_token();
}

int SignalStopMonitor::ReceivedSignal() const noexcept {
  return received_signal_.load(std::memory_order_acquire);
}

void SignalStopMonitor::Run(std::stop_token stop_token) noexcept {
  pollfd descriptor{
      .fd = signal_fd_,
      .events = POLLIN,
      .revents = 0,
  };
  while (!stop_token.stop_requested()) {
    descriptor.revents = 0;
    const int ready = poll(&descriptor, 1, 100);
    if (ready < 0) {
      if (errno == EINTR) {
        continue;
      }
      return;
    }
    if (ready == 0) {
      continue;
    }
    if ((descriptor.revents & POLLIN) == 0) {
      return;
    }

    while (true) {
      signalfd_siginfo info{};
      const ssize_t received = read(signal_fd_, &info, sizeof(info));
      if (received < 0) {
        if (errno == EINTR) {
          continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
          break;
        }
        return;
      }
      if (received != static_cast<ssize_t>(sizeof(info))) {
        return;
      }
      if (info.ssi_signo != static_cast<std::uint32_t>(SIGINT) &&
          info.ssi_signo != static_cast<std::uint32_t>(SIGTERM)) {
        continue;
      }
      int expected = 0;
      static_cast<void>(received_signal_.compare_exchange_strong(
          expected, static_cast<int>(info.ssi_signo), std::memory_order_release,
          std::memory_order_relaxed));
      stop_source_.request_stop();
    }
  }
}

}  // namespace bbp
