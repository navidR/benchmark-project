#include <boost/test/unit_test.hpp>

#include "bbp/operator_command_status.h"

BOOST_AUTO_TEST_CASE(operator_command_status_round_trips_names) {
  constexpr bbp::OperatorCommandStatus kStatuses[] = {
      bbp::OperatorCommandStatus::kCompleted,
      bbp::OperatorCommandStatus::kFailed,
  };

  for (bbp::OperatorCommandStatus status : kStatuses) {
    const std::optional<bbp::OperatorCommandStatus> parsed =
        bbp::OperatorCommandStatusFromName(
            bbp::OperatorCommandStatusName(status));
    BOOST_REQUIRE(parsed);
    BOOST_CHECK(*parsed == status);
  }
  BOOST_TEST(!bbp::OperatorCommandStatusFromName("unknown"));
}
