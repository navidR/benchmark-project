#pragma once

#include <stop_token>

namespace bbp::simulator_app_internal {

struct StopForwarder {
  std::stop_source* target = nullptr;

  void operator()() const noexcept { target->request_stop(); }
};

class CombinedStopToken {
 public:
  CombinedStopToken(std::stop_token first, std::stop_token second);

  [[nodiscard]] std::stop_token get_token() const noexcept;

 private:
  std::stop_source source_;
  std::stop_callback<StopForwarder> first_callback_;
  std::stop_callback<StopForwarder> second_callback_;
};

}  // namespace bbp::simulator_app_internal
