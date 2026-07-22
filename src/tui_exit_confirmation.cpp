#include "bbp/tui_exit_confirmation.h"

namespace bbp {

TuiExitConfirmationResult TuiExitConfirmation::HandleInput(int ch) {
  if (exit_requested_ || ch < 0) {
    return TuiExitConfirmationResult::kIgnored;
  }
  if (!open_) {
    if (ch != 27) {
      return TuiExitConfirmationResult::kIgnored;
    }
    open_ = true;
    return TuiExitConfirmationResult::kOpened;
  }
  if (ch == 'y' || ch == 'Y') {
    open_ = false;
    exit_requested_ = true;
    return TuiExitConfirmationResult::kConfirmed;
  }
  if (ch == 'n' || ch == 'N' || ch == 27) {
    open_ = false;
    return TuiExitConfirmationResult::kCancelled;
  }
  return TuiExitConfirmationResult::kConsumed;
}

}  // namespace bbp
