#include <boost/test/unit_test.hpp>
#include <chrono>

#include "bbp/simulation_command_queue.h"
#include "bbp/tui_command_admission.h"

BOOST_AUTO_TEST_CASE(
    tui_command_admission_remains_responsive_after_queue_overload) {
  bbp::SimulationCommandQueue queue(1U);
  BOOST_TEST(queue.Push(bbp::SimulationCommandKind::kExportNodeReport,
                        "firo-1") == 1U);

  constexpr std::size_t kRejectedAttempts = 10'000U;
  const auto started = std::chrono::steady_clock::now();
  for (std::size_t attempt = 0U; attempt < kRejectedAttempts; ++attempt) {
    const bbp::TuiCommandAdmissionResult result = bbp::AdmitTuiCommand([&] {
      return queue.Push(bbp::SimulationCommandKind::kExportNodeReport,
                        "firo-1");
    });
    BOOST_TEST(!result.accepted);
    BOOST_TEST(result.sequence == 0U);
    if (attempt == 0U || attempt + 1U == kRejectedAttempts) {
      BOOST_TEST(
          result.feedback ==
          "Command rejected: simulation command queue is full (capacity 1)");
    }
  }
  const auto elapsed = std::chrono::steady_clock::now() - started;
  BOOST_TEST(elapsed < std::chrono::seconds(5));
  BOOST_TEST(queue.Stats().rejected == kRejectedAttempts);

  BOOST_REQUIRE(queue.TryPop());
  const bbp::TuiCommandAdmissionResult recovered = bbp::AdmitTuiCommand([&] {
    return queue.Push(bbp::SimulationCommandKind::kExportNodeReport, "firo-1");
  });
  BOOST_TEST(recovered.accepted);
  BOOST_TEST(recovered.sequence == 2U);
  BOOST_TEST(recovered.feedback.empty());
}
