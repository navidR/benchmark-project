#include <unistd.h>

#include <atomic>
#include <boost/json.hpp>
#include <boost/test/unit_test.hpp>
#include <filesystem>
#include <iomanip>
#include <sstream>
#include <thread>

#include "bbp/chain_view.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/tui_chain_pane.h"
#include "bbp/util.h"

namespace {
std::string Hash(std::uint64_t height, unsigned branch = 0) {
  std::ostringstream out;
  out << std::hex << std::setfill('0') << std::setw(32) << branch
      << std::setw(32) << height;
  return out.str();
}
struct ChainFixture {
  std::filesystem::path root = std::filesystem::temp_directory_path() /
                               ("bbp-chain-view-" + std::to_string(getpid()));
  std::atomic<std::uint64_t> tip{150}, fork{151};
  std::atomic<unsigned> branch{0};
  std::atomic<bool> first_healthy{true}, available{true};
  std::atomic<std::size_t> reads{0};
  unsigned transaction_count = 0;
  ChainFixture() { std::filesystem::create_directories(root); }
  ~ChainFixture() { std::filesystem::remove_all(root); }
  bbp::ChainBlockSummary Summary(std::uint64_t height) {
    ++reads;
    bbp::ChainBlockSummary b;
    b.height = height;
    b.hash = Hash(height, height >= fork ? branch.load() : 0);
    b.confirmations = static_cast<std::int64_t>(tip - height + 1);
    if (height)
      b.previous_hash =
          Hash(height - 1, height - 1 >= fork ? branch.load() : 0);
    return b;
  }
  bbp::ChainViewService::Readers Readers() {
    return [this] {
      std::vector<bbp::ChainBlockReader> readers;
      for (const std::string node : {"one", "two"})
        readers.push_back(
            {.node_id = node,
             .tip =
                 [this, node](std::stop_token) {
                   if (!available || (node == "one" && !first_healthy))
                     throw std::runtime_error("offline");
                   return Summary(tip);
                 },
             .summary = [this](std::uint64_t height,
                               std::stop_token) { return Summary(height); },
             .detail =
                 [this](const std::string& hash, std::stop_token) {
                   if (hash == Hash(0))
                     throw std::runtime_error("block pruned");
                   bbp::ChainBlockDetail d;
                   d.block = Summary(std::stoull(hash.substr(32), nullptr, 16));
                   d.byte_definition = "test serialized bytes";
                   for (unsigned i = 1; i <= transaction_count; ++i) {
                     bbp::ChainBlockTransaction tx;
                     tx.id = Hash(i);
                     tx.serialized_size = 100 + i;
                     tx.fee = std::to_string(i) + " test units";
                     tx.error = std::string(200, 'x') + " END-OF-TRANSACTION";
                     d.transactions.push_back(std::move(tx));
                   }
                   d.block.transaction_count = transaction_count;
                   bbp::CalculateBlockTransactionSizes(d);
                   return d;
                 }});
      return readers;
    };
  }
};
}  // namespace

// Protects sparse lazy reads, canonical hash identity after reorg, and an
// offline reader never falling through to live RPC for missing captures.
BOOST_AUTO_TEST_CASE(chain_view_lazy_reorg_and_retained_records) {
  ChainFixture f;
  bbp::ChainViewService service(f.root, f.Readers());
  auto page =
      service.Query({.first_height = 140, .selected_height = 145, .limit = 10});
  BOOST_TEST(f.reads.load() < 20U);
  BOOST_TEST(page.at("source_node").as_string() == "one");
  BOOST_TEST(page.at("detail")
                 .as_object()
                 .at("block")
                 .as_object()
                 .at("hash")
                 .as_string() == Hash(145));
  f.tip = 152;
  f.fork = 144;
  f.branch = 1;
  page =
      service.Query({.first_height = 140, .selected_height = 145, .limit = 10});
  BOOST_TEST(page.at("notice").as_string().find("Reorg") !=
             boost::json::string::npos);
  BOOST_TEST(page.at("detail")
                 .as_object()
                 .at("block")
                 .as_object()
                 .at("hash")
                 .as_string() == Hash(145, 1));
  // A reorg that shortens the chain must remove the former successor from
  // cached details when this selected block becomes the new tip.
  f.tip = 145;
  page =
      service.Query({.first_height = 145, .selected_height = 145, .limit = 1});
  const auto& tip_block = page.at("detail").as_object().at("block").as_object();
  BOOST_TEST(tip_block.at("next_hash").is_null());
  BOOST_TEST(bbp::JsonUint(tip_block, "confirmations") == 1U);
  service.Close();
  const auto reads = f.reads.load();
  bbp::ChainViewService retained(f.root);
  auto captured = retained.Query(
      {.first_height = 140, .selected_height = 145, .limit = 10});
  BOOST_TEST(captured.at("mode").as_string() == "retained");
  BOOST_TEST(captured.at("detail")
                 .as_object()
                 .at("block")
                 .as_object()
                 .at("hash")
                 .as_string() == Hash(145, 1));
  auto missing =
      retained.Query({.first_height = 0, .selected_height = 0, .limit = 1});
  BOOST_TEST(!missing.at("detail_error").as_string().empty());
  BOOST_TEST(f.reads.load() == reads);
}

// Browsing a long chain cannot retain every visited block; failure changes
// source visibly, and unavailable blocks do not poison other rows.
BOOST_AUTO_TEST_CASE(chain_view_bound_source_failure_and_pruning) {
  ChainFixture f;
  bbp::ChainViewService service(f.root, f.Readers());
  for (std::uint64_t height = 0; height < 140; height += 16) {
    auto page = service.Query(
        {.first_height = height, .selected_height = height, .limit = 16});
    BOOST_TEST(page.at("cache_entries").as_uint64() <= 64U);
    if (height == 0) {
      BOOST_TEST(page.at("detail_error").as_string() == "block pruned");
      BOOST_TEST(page.at("rows")
                     .as_array()
                     .front()
                     .as_object()
                     .at("error")
                     .as_string() == "block pruned");
    }
  }
  f.first_healthy = false;
  auto page = service.Query({});
  BOOST_TEST(page.at("source_node").as_string() == "two");
  BOOST_TEST(page.at("notice").as_string().find("one -> two") !=
             boost::json::string::npos);
  const auto records =
      boost::json::parse(bbp::ReadText(f.root / "chain-blocks.json"));
  BOOST_TEST(records.as_object().at("records").as_array().size() <= 64U);
}

// Exercise the same selection and asynchronous reader used by curses, with
// growth while pinned, tip following, empty/genesis and narrow rendering.
BOOST_AUTO_TEST_CASE(chain_view_navigation_growth_and_clipping) {
  ChainFixture f;
  auto service = std::make_shared<bbp::ChainViewService>(f.root, f.Readers());
  bbp::TuiChainPane pane;
  const auto wait = [&](auto predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
      pane.Refresh(service, 36);
      if (predicate()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
  };
  BOOST_REQUIRE(wait([&] { return pane.selected_height() == 150; }));
  pane.Navigate(bbp::ChainNavigation::kUp);
  BOOST_TEST(pane.selected_height().value() == 149U);
  BOOST_TEST(!pane.following_tip());
  f.tip = 151;
  const auto prior = pane.revision();
  BOOST_REQUIRE(wait([&] { return pane.revision() > prior; }));
  BOOST_TEST(pane.selected_height().value() == 149U);
  f.available = false;
  BOOST_REQUIRE(wait([&] {
    const auto lines = pane.Lines(36, 100);
    return lines.at(2).text.find("offline") != std::string::npos;
  }));
  f.available = true;
  BOOST_REQUIRE(wait([&] {
    const auto lines = pane.Lines(36, 100);
    return lines.at(1).text.find("Tip: 151") != std::string::npos;
  }));
  BOOST_TEST(pane.selected_height().value() == 149U);
  pane.Navigate(bbp::ChainNavigation::kHome);
  BOOST_TEST(pane.selected_height().value() == 0U);
  pane.Navigate(bbp::ChainNavigation::kPageDown);
  BOOST_TEST(pane.selected_height().value() > 0U);
  pane.Navigate(bbp::ChainNavigation::kPageUp);
  BOOST_TEST(pane.selected_height().value() == 0U);
  pane.Navigate(bbp::ChainNavigation::kEnd);
  BOOST_REQUIRE(wait([&] { return pane.selected_height() == 151; }));
  BOOST_TEST(pane.following_tip());
  f.tip = 152;
  BOOST_REQUIRE(wait([&] { return pane.selected_height() == 152; }));
  for (const auto [rows, columns] :
       {std::pair{1, 1}, std::pair{12, 20}, std::pair{24, 80}}) {
    const auto lines = pane.Lines(rows, columns);
    BOOST_TEST(lines.size() == static_cast<std::size_t>(rows));
    BOOST_TEST(std::all_of(lines.begin(), lines.end(), [&](const auto& line) {
      return line.text.size() <= static_cast<std::size_t>(columns);
    }));
  }
  pane.Reset();
  f.tip = 0;
  BOOST_REQUIRE(wait([&] { return pane.selected_height() == 0; }));
  BOOST_TEST(pane.following_tip());
  pane.Cancel();
  service->Close();
  auto empty = std::make_shared<bbp::ChainViewService>(f.root / "missing");
  for (int i = 0; i < 20; ++i) {
    pane.Refresh(empty, 24);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  }
  BOOST_TEST(!pane.selected_height().has_value());
}

// A slow obsolete page must be interrupted by navigation, not block drawing
// or publish its result over the newly selected tip.
BOOST_AUTO_TEST_CASE(chain_view_navigation_cancels_obsolete_rpc) {
  ChainFixture f;
  std::atomic<bool> blocked = false, cancelled = false;
  auto factory = f.Readers();
  auto service = std::make_shared<bbp::ChainViewService>(f.root, [&] {
    auto readers = factory();
    for (auto& reader : readers) {
      auto original = reader.summary;
      reader.summary = [&, original](std::uint64_t height,
                                     std::stop_token stop) {
        if (height == 0) {
          blocked = true;
          std::mutex mutex;
          std::condition_variable_any condition;
          std::unique_lock lock(mutex);
          condition.wait(lock, stop, [] { return false; });
          cancelled = true;
          throw bbp::SimulationCancelled();
        }
        return original(height, stop);
      };
    }
    return readers;
  });
  bbp::TuiChainPane pane;
  const auto wait = [&](auto predicate) {
    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(3);
    do {
      pane.Refresh(service, 36);
      if (predicate()) return true;
      std::this_thread::sleep_for(std::chrono::milliseconds(5));
    } while (std::chrono::steady_clock::now() < deadline);
    return false;
  };
  BOOST_REQUIRE(wait([&] { return pane.selected_height() == 150; }));
  pane.Navigate(bbp::ChainNavigation::kHome);
  BOOST_REQUIRE(wait([&] { return blocked.load(); }));
  BOOST_TEST(!pane.Lines(24, 80).empty());
  pane.Navigate(bbp::ChainNavigation::kEnd);
  BOOST_REQUIRE(
      wait([&] { return cancelled.load() && pane.selected_height() == 150; }));
}

// A block longer than the viewport must still expose its final transaction
// and every wrapped detail field, with valid focus/selection after resize.
BOOST_AUTO_TEST_CASE(chain_view_responsive_transaction_browser) {
  ChainFixture f;
  f.transaction_count = 80;
  auto service = std::make_shared<bbp::ChainViewService>(f.root, f.Readers());
  bbp::TuiChainPane pane;
  const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(3);
  do {
    pane.Refresh(service, 44, 80);
    if (pane.selected_height()) break;
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
  } while (std::chrono::steady_clock::now() < until);
  BOOST_REQUIRE(pane.selected_height().has_value());
  const auto joined = [](const auto& rows) {
    std::string result;
    for (const auto& row : rows) result += row.text + "\n";
    return result;
  };
  auto wide = joined(pane.Lines(44, 80));
  BOOST_TEST(wide.find("3 Blocks") != std::string::npos);
  BOOST_TEST(wide.find("4 Block transactions") != std::string::npos);
  BOOST_TEST(wide.find("Weight") != std::string::npos);
  pane.FocusTransactions();
  pane.Navigate(bbp::ChainNavigation::kEnd);
  pane.Navigate(bbp::ChainNavigation::kInspect);
  wide = joined(pane.Lines(44, 80));
  BOOST_TEST(wide.find("fee: 80 test units") != std::string::npos);
  BOOST_TEST(wide.find("4 Transaction details") != std::string::npos);
  for (const auto [rows, columns] : {std::pair{24, 80}, std::pair{12, 40}}) {
    pane.Lines(rows, columns);
    pane.Navigate(bbp::ChainNavigation::kEnd);
    const auto screen = pane.Lines(rows, columns);
    BOOST_TEST(joined(screen).find("END-OF-TRANSACTION") != std::string::npos);
    BOOST_TEST(screen.size() == static_cast<std::size_t>(rows));
    BOOST_TEST(std::all_of(screen.begin(), screen.end(), [&](const auto& row) {
      return row.text.size() <= static_cast<std::size_t>(columns);
    }));
    pane.Navigate(bbp::ChainNavigation::kHome);
  }
  // Detail scrolling must not change the selected transaction.
  wide = joined(pane.Lines(44, 80));
  BOOST_TEST(wide.find("fee: 80 test units") != std::string::npos);
  pane.Navigate(bbp::ChainNavigation::kBack);
  BOOST_TEST(joined(pane.Lines(44, 80)).find("4 Block transactions") !=
             std::string::npos);
  pane.Navigate(bbp::ChainNavigation::kInspect);
  BOOST_TEST(joined(pane.Lines(44, 80)).find("fee: 80 test units") !=
             std::string::npos);
}
