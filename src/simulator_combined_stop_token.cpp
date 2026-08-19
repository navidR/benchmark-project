#include "simulator_combined_stop_token.h"

#include <stop_token>

namespace bbp::simulator_app_internal {

CombinedStopToken::CombinedStopToken(std::stop_token first,
                                     std::stop_token second)
    : first_callback_(first, StopForwarder{&source_}),
      second_callback_(second, StopForwarder{&source_}) {}

std::stop_token CombinedStopToken::get_token() const noexcept {
  return source_.get_token();
}

}  // namespace bbp::simulator_app_internal
