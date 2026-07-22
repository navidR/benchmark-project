#include <array>
#include <boost/test/unit_test.hpp>

#include "bbp/tui_exit_confirmation.h"

BOOST_AUTO_TEST_CASE(tui_exit_confirmation_requires_explicit_confirmation) {
  bbp::TuiExitConfirmation confirmation;
  BOOST_CHECK(confirmation.HandleInput('q') ==
              bbp::TuiExitConfirmationResult::kIgnored);
  BOOST_TEST(!confirmation.is_open());
  BOOST_TEST(!confirmation.exit_requested());

  BOOST_CHECK(confirmation.HandleInput(27) ==
              bbp::TuiExitConfirmationResult::kOpened);
  BOOST_TEST(confirmation.is_open());
  BOOST_CHECK(confirmation.HandleInput('x') ==
              bbp::TuiExitConfirmationResult::kConsumed);
  BOOST_TEST(confirmation.is_open());
  BOOST_CHECK(confirmation.HandleInput(-1) ==
              bbp::TuiExitConfirmationResult::kIgnored);
  BOOST_TEST(confirmation.is_open());
  BOOST_CHECK(confirmation.HandleInput('y') ==
              bbp::TuiExitConfirmationResult::kConfirmed);
  BOOST_TEST(!confirmation.is_open());
  BOOST_TEST(confirmation.exit_requested());
}

BOOST_AUTO_TEST_CASE(tui_exit_confirmation_cancel_preserves_active_state) {
  constexpr std::array<int, 3> kCancelInputs{'n', 'N', 27};
  for (const int cancel : kCancelInputs) {
    bbp::TuiExitConfirmation confirmation;
    BOOST_CHECK(confirmation.HandleInput(27) ==
                bbp::TuiExitConfirmationResult::kOpened);
    BOOST_CHECK(confirmation.HandleInput(cancel) ==
                bbp::TuiExitConfirmationResult::kCancelled);
    BOOST_TEST(!confirmation.is_open());
    BOOST_TEST(!confirmation.exit_requested());
    BOOST_CHECK(confirmation.HandleInput(27) ==
                bbp::TuiExitConfirmationResult::kOpened);
    BOOST_CHECK(confirmation.HandleInput('Y') ==
                bbp::TuiExitConfirmationResult::kConfirmed);
    BOOST_TEST(confirmation.exit_requested());
  }
}
