#include <unistd.h>

#include <atomic>
#include <boost/json.hpp>
#include <boost/test/unit_test.hpp>
#include <iomanip>
#include <sstream>
#include <thread>

#include "bbp/pool_view.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/tui_pool_pane.h"
#include "bbp/util.h"

namespace {
std::string Id(unsigned value) {
  std::ostringstream out;
  out << std::hex << std::setw(64) << std::setfill('0') << value;
  return out.str();
}
struct PoolFixture {
  std::filesystem::path root = std::filesystem::temp_directory_path() /
                               ("bbp-pool-view-" + std::to_string(getpid()));
  std::mutex mutex;
  bbp::ChainPoolSnapshot pool;
  std::atomic<bool> first_healthy{true}, available{true};
  std::atomic<bool> stall{false}, entered{false}, cancelled{false};
  std::atomic<unsigned> reads{0};
  std::string reason;
  PoolFixture() {
    std::filesystem::create_directories(root);
    SetCount(40);
  }
  ~PoolFixture() { std::filesystem::remove_all(root); }
  void SetCount(unsigned count) {
    std::lock_guard lock(mutex);
    pool.transactions.clear();
    for (unsigned i = 1; i <= count; ++i) {
      bbp::ChainPoolTransaction tx;
      tx.id = Id(i);
      tx.fee = i;
      tx.serialized_size = 200;
      pool.transactions.push_back(std::move(tx));
    }
    bbp::CalculatePoolSummary(pool);
  }
  bbp::PoolViewService::Readers Readers() {
    return [this] {
      std::vector<bbp::PoolReader> readers;
      for (const std::string node : {"one", "two"})
        readers.push_back(
            {.node_id = node,
             .snapshot =
                 [this, node](std::stop_token) {
                   ++reads;
                   if (!available || (node == "one" && !first_healthy))
                     throw std::runtime_error("offline");
                   std::lock_guard lock(mutex);
                   return pool;
                 },
             .detail =
                 [this](const bbp::ChainPoolTransaction& tx,
                        std::stop_token stop) {
                   if (stall) {
                     entered = true;
                     while (!stop.stop_requested() && stall)
                       std::this_thread::sleep_for(
                           std::chrono::milliseconds(1));
                     if (stop.stop_requested()) {
                       cancelled = true;
                       throw bbp::SimulationCancelled();
                     }
                   }
                   auto detail = tx;
                   detail.input_count = 2;
                   return detail;
                 },
             .departure = [this](const std::string&,
                                 std::stop_token) { return reason; }});
      return readers;
    };
  }
};
template <typename Predicate>
bool Wait(Predicate predicate) {
  const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  do {
    if (predicate()) return true;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  } while (std::chrono::steady_clock::now() < until);
  return false;
}
}  // namespace

// Detect attaching another transaction's details after insertion/removal,
// losing known departure reasons, or retaining unbounded snapshot history.
BOOST_AUTO_TEST_CASE(pool_view_churn_identity_and_retained_bound) {
  PoolFixture f;
  f.SetCount(2000);
  bbp::PoolViewService service(f.root, f.Readers());
  auto page =
      service.Query({.selected_id = Id(1000), .index = 999, .limit = 8});
  BOOST_TEST(page.at("rows").as_array().size() == 8U);
  BOOST_TEST(page.at("selected_id").as_string() == Id(1000));
  {
    std::lock_guard lock(f.mutex);
    bbp::ChainPoolTransaction tx;
    tx.id = Id(0);
    f.pool.transactions.push_back(tx);
    bbp::CalculatePoolSummary(f.pool);
  }
  page = service.Query({.selected_id = Id(1000), .index = 999, .limit = 8});
  BOOST_TEST(bbp::JsonUint(page, "selected_index") == 1000U);
  BOOST_TEST(page.at("detail").as_object().at("id").as_string() == Id(1000));
  for (const std::string reason :
       {"Mined in block abc", "Replaced by transaction def", ""}) {
    f.reason = reason;
    f.SetCount(3);
    page = service.Query({.selected_id = Id(1000), .index = 1000, .limit = 8});
    BOOST_TEST(page.at("departed_id").as_string() == Id(1000));
    BOOST_TEST(page.at("selected_id").as_string() == Id(3));
    BOOST_TEST(page.at("departure_reason").as_string() ==
               (reason.empty() ? "Left pool; removal reason unknown" : reason));
  }
  const auto sample_time = page.at("sampled_at_ms");
  service.Close();
  const auto reads = f.reads.load();
  bbp::PoolViewService retained(f.root);
  page = retained.Query({.selected_id = Id(3)});
  BOOST_TEST(page.at("mode").as_string() == "retained");
  BOOST_TEST(page.at("sampled_at_ms") == sample_time);
  BOOST_TEST(bbp::JsonUint(page.at("detail").as_object(), "input_count") == 2U);
  const auto uncaptured = retained.Query({.selected_id = Id(1)});
  BOOST_TEST(!uncaptured.at("detail_error").as_string().empty());
  BOOST_TEST(f.reads.load() == reads);
  const auto capture =
      boost::json::parse(bbp::ReadText(f.root / "transaction-pool.json"));
  BOOST_TEST(capture.as_object().at("transactions").as_array().size() == 3U);
  BOOST_TEST(std::filesystem::file_size(f.root / "transaction-pool.json") <
             bbp::PoolViewService::kMaximumBytes);
}

// Failover must be explicit, sticky and never turn source disagreement into
// a false eviction/mining verdict. Empty and oversized samples remain bounded.
BOOST_AUTO_TEST_CASE(pool_view_sources_empty_and_limit) {
  PoolFixture f;
  bbp::PoolViewService service(f.root, f.Readers());
  BOOST_TEST(service.Query({}).at("source_node").as_string() == "one");
  f.first_healthy = false;
  f.SetCount(0);
  f.reason = "Mined";
  auto page = service.Query({.selected_id = Id(1)});
  BOOST_TEST(page.at("source_node").as_string() == "two");
  BOOST_TEST(page.at("notice").as_string().find("one -> two") !=
             boost::json::string::npos);
  BOOST_TEST(page.at("departure_reason").as_string().find("new source") !=
             boost::json::string::npos);
  BOOST_TEST(page.at("rows").as_array().empty());
  BOOST_TEST(page.at("detail").is_null());
  f.first_healthy = true;
  BOOST_TEST(service.Query({}).at("source_node").as_string() == "two");
  f.available = false;
  BOOST_TEST(!service.Query({}).at("error").as_string().empty());
  f.available = true;
  f.SetCount(4);
  BOOST_TEST(service.Query({}).at("rows").as_array().size() == 4U);
  BOOST_CHECK_THROW(service.Query({.selected_id = "invalid"}),
                    std::invalid_argument);
  BOOST_CHECK_THROW(service.Query({.selected_id = {}, .limit = 33}),
                    std::invalid_argument);
  f.SetCount(0);
  f.pool.transactions.resize(bbp::ChainPoolSnapshot::kMaximumTransactions + 1);
  BOOST_TEST(!service.Query({}).at("error").as_string().empty());
}

// The real UI worker must cancel obsolete reads, retain selection through
// failure/churn, and permit keyboard navigation while RPC is pending.
BOOST_AUTO_TEST_CASE(pool_view_navigation_cancellation_and_clipping) {
  PoolFixture f;
  auto service = std::make_shared<bbp::PoolViewService>(f.root, f.Readers());
  bbp::TuiPoolPane pane;
  const auto selected = [&](const std::string& id) {
    return Wait([&] {
      pane.Refresh(service, 36);
      return pane.selected_id() == id;
    });
  };
  BOOST_REQUIRE(selected(Id(1)));
  pane.Navigate(bbp::PoolNavigation::kDown);
  BOOST_REQUIRE(selected(Id(2)));
  pane.Navigate(bbp::PoolNavigation::kPageDown);
  BOOST_REQUIRE(selected(Id(11)));
  pane.Navigate(bbp::PoolNavigation::kPageUp);
  BOOST_REQUIRE(selected(Id(2)));
  pane.Navigate(bbp::PoolNavigation::kHome);
  BOOST_REQUIRE(selected(Id(1)));
  f.stall = true;
  BOOST_REQUIRE(Wait([&] {
    pane.Refresh(service, 36);
    return f.entered.load();
  }));
  const auto before = std::chrono::steady_clock::now();
  pane.Navigate(bbp::PoolNavigation::kEnd);
  BOOST_REQUIRE(Wait([&] { return f.cancelled.load(); }));
  BOOST_CHECK(std::chrono::steady_clock::now() - before <
              std::chrono::milliseconds(500));
  f.stall = false;
  BOOST_REQUIRE(selected(Id(40)));
  f.available = false;
  BOOST_REQUIRE(Wait([&] {
    pane.Refresh(service, 36);
    return pane.Lines(36, 80)[2].text.find("offline") != std::string::npos;
  }));
  BOOST_TEST(pane.selected_id() == Id(40));
  f.available = true;
  f.reason = "Mined";
  f.SetCount(39);
  BOOST_REQUIRE(selected(Id(39)));
  BOOST_TEST(pane.Lines(36, 100)[2].text.find("Mined") != std::string::npos);
  for (int width : {1, 20, 80}) {
    for (const auto& line : pane.Lines(12, width))
      BOOST_TEST(line.text.size() <= static_cast<std::size_t>(width));
  }
  for (unsigned focus = 1; focus <= 2; ++focus) {
    pane.Navigate(bbp::PoolNavigation::kFocus);
    pane.Navigate(bbp::PoolNavigation::kEnd);
    pane.Navigate(bbp::PoolNavigation::kHome);
  }
  BOOST_TEST(pane.selected_id() == Id(39));
}
