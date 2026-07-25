#include <boost/test/unit_test.hpp>
#include <cstddef>
#include <string>
#include <vector>

#include "bbp/simulator_log_pane.h"

namespace {

const std::string kFiroQtCommand =
    "[2026-07-25 11:20:00.000000] [info] manual Firo GUI command: "
    "'/opt/Firo GUI/firo-qt' '-regtest' "
    "'-datadir=/tmp/bbp-firo-regression/operator/firo-qt' "
    "'-connect=127.0.0.1:18168' '-dns=0' '-dnsseed=0' "
    "'-forcednsseed=0' '-maxconnections=1' '-listen=0' '-discover=0' "
    "'-listenonion=0' '-torsetup=0' '-upnp=0'";

std::string ReassembleRecord(
    const std::vector<bbp::SimulatorLogVisualRow>& rows,
    std::size_t record_index) {
  std::string result;
  for (const bbp::SimulatorLogVisualRow& row : rows) {
    if (row.record_index == record_index) {
      result += row.text;
    }
  }
  return result;
}

void RequireValidRows(const bbp::SimulatorLogPane& pane,
                      const std::vector<std::string>& records,
                      std::size_t width) {
  std::vector<std::size_t> starts(records.size(), 0U);
  for (const bbp::SimulatorLogVisualRow& row : pane.Rows()) {
    BOOST_TEST(row.text.size() <= width);
    BOOST_REQUIRE(row.record_index < records.size());
    if (row.starts_record) {
      ++starts[row.record_index];
      BOOST_TEST(row.byte_offset == 0U);
    }
  }
  for (std::size_t index = 0U; index < records.size(); ++index) {
    BOOST_TEST(starts[index] == 1U);
    BOOST_TEST(ReassembleRecord(pane.Rows(), index) == records[index]);
  }
}

}  // namespace

BOOST_AUTO_TEST_CASE(simulator_log_pane_wraps_complete_records_at_word_edges) {
  const std::vector<std::string> records = {
      "short record",
      kFiroQtCommand,
      "prefix abcdefghijklmnopqrstuvwxyz0123456789 suffix",
      "",
  };
  bbp::SimulatorLogPane pane;
  pane.Refresh(records, 18U, 5U);

  RequireValidRows(pane, records, 18U);
  BOOST_REQUIRE(pane.Rows().size() > records.size());
  BOOST_TEST(pane.Rows()[0].text == "short record");
  BOOST_TEST(pane.Rows()[1].text == "[2026-07-25 ");
  BOOST_TEST(pane.Rows()[1].starts_record);
  BOOST_TEST(!pane.Rows()[2].starts_record);

  bool found_hard_wrapped_token = false;
  for (const bbp::SimulatorLogVisualRow& row : pane.Rows()) {
    if (row.record_index == 2U && row.text.size() == 18U &&
        row.text.find(' ') == std::string::npos) {
      found_hard_wrapped_token = true;
    }
  }
  BOOST_TEST(found_hard_wrapped_token);
}

BOOST_AUTO_TEST_CASE(simulator_log_pane_scrolls_by_visual_rows) {
  bbp::SimulatorLogPane pane;
  pane.Refresh({kFiroQtCommand}, 18U, 4U);
  const std::size_t tail = pane.FirstVisibleRow();
  BOOST_REQUIRE(tail > 2U);
  BOOST_TEST(pane.IsFollowingTail());

  pane.ScrollUp(1U);
  BOOST_TEST(pane.FirstVisibleRow() == tail - 1U);
  BOOST_TEST(!pane.IsFollowingTail());
  pane.ScrollUp(2U);
  BOOST_TEST(pane.FirstVisibleRow() == tail - 3U);
  pane.ScrollDown(1U);
  BOOST_TEST(pane.FirstVisibleRow() == tail - 2U);
  pane.ScrollHome();
  BOOST_TEST(pane.FirstVisibleRow() == 0U);
  pane.ScrollEnd();
  BOOST_TEST(pane.FirstVisibleRow() == tail);
  BOOST_TEST(pane.IsFollowingTail());
}

BOOST_AUTO_TEST_CASE(
    simulator_log_pane_reflows_from_same_record_byte_on_resize) {
  const std::vector<std::string> records = {
      "earlier record",
      kFiroQtCommand,
      "later record that keeps the command away from the live tail",
  };
  bbp::SimulatorLogPane pane;
  pane.Refresh(records, 18U, 4U);
  pane.ScrollHome();
  pane.ScrollDown(1U);
  BOOST_REQUIRE(pane.FirstVisibleRow() < pane.Rows().size());
  const bbp::SimulatorLogVisualRow before = pane.Rows()[pane.FirstVisibleRow()];

  pane.Refresh(records, 31U, 6U);
  BOOST_REQUIRE(pane.FirstVisibleRow() < pane.Rows().size());
  const bbp::SimulatorLogVisualRow& after = pane.Rows()[pane.FirstVisibleRow()];
  BOOST_TEST(after.record_index == before.record_index);
  BOOST_TEST(after.byte_offset <= before.byte_offset);
  BOOST_TEST(before.byte_offset <= after.byte_offset + after.text.size());
  RequireValidRows(pane, records, 31U);
}
