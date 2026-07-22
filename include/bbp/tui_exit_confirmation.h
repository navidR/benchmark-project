#pragma once

namespace bbp {

enum class TuiExitConfirmationResult {
  kIgnored,
  kOpened,
  kCancelled,
  kConfirmed,
  kConsumed,
};

class TuiExitConfirmation {
 public:
  TuiExitConfirmationResult HandleInput(int ch);

  [[nodiscard]] bool is_open() const noexcept { return open_; }
  [[nodiscard]] bool exit_requested() const noexcept { return exit_requested_; }

 private:
  bool open_ = false;
  bool exit_requested_ = false;
};

}  // namespace bbp
