#include <unistd.h>

#include <atomic>
#include <boost/asio/ip/tcp.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <filesystem>
#include <future>
#include <mutex>
#include <stop_token>
#include <thread>

#include "bbp/drivers/firo_driver.h"
#include "bbp/simulation_cancelled.h"

BOOST_AUTO_TEST_CASE(firo_process_does_not_persist_simulation_peers) {
  const std::filesystem::path test_dir =
      std::filesystem::temp_directory_path() /
      ("bbp-firo-process-peers-" + std::to_string(getpid()));
  std::filesystem::remove_all(test_dir);

  bbp::FiroNodeConfig config;
  config.id = "peer-argument-test";
  config.binary = "/usr/bin/firod";
  config.data_dir = test_dir / "data";
  config.log_dir = test_dir / "logs";
  config.rpc_port = 18888U;
  config.p2p_port = 18168U;
  config.rpc_user = "user";
  config.rpc_password = "password";
  config.connect_peers = {"127.0.0.1:18169"};

  const bbp::FiroDriver driver(std::chrono::milliseconds(100));
  const bbp::ProcessSpec process = driver.RenderProcess(config);
  for (const std::string& argument : process.argv) {
    BOOST_TEST(!argument.starts_with("-connect="));
  }

  std::filesystem::remove_all(test_dir);
}

BOOST_AUTO_TEST_CASE(
    firo_peer_identity_rejects_multiple_candidates_on_the_same_host) {
  bbp::FiroNodeConfig config;
  config.id = "ambiguous-peer-test";

  const bbp::FiroDriver driver(std::chrono::milliseconds(100));
  BOOST_CHECK_THROW(driver.ConnectedPeerAddresses(
                        config, {"127.0.0.1:18168", "127.0.0.1:18169"}),
                    bbp::UnsupportedChainOperation);
}

BOOST_AUTO_TEST_CASE(firo_readiness_wait_honors_stop_token_promptly) {
  bbp::FiroNodeConfig config;
  config.id = "cancel-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = 1U;
  config.rpc_user = "user";
  config.rpc_password = "password";

  bbp::FiroDriver driver(std::chrono::milliseconds(100));
  std::stop_source stop_source;
  std::atomic<bool> cancelled = false;
  std::exception_ptr failure;
  std::thread waiter([&] {
    try {
      driver.WaitReady(config, std::chrono::hours(24), stop_source.get_token());
    } catch (const bbp::SimulationCancelled&) {
      cancelled = true;
    } catch (...) {
      failure = std::current_exception();
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  const auto stop_started = std::chrono::steady_clock::now();
  stop_source.request_stop();
  waiter.join();
  const auto stop_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - stop_started);

  BOOST_TEST(!static_cast<bool>(failure));
  BOOST_TEST(cancelled.load());
  BOOST_TEST(stop_duration.count() < 500);
}

BOOST_AUTO_TEST_CASE(firo_rpc_wait_honors_stop_while_server_is_silent) {
  namespace asio = boost::asio;
  using tcp = asio::ip::tcp;

  asio::io_context server_context;
  tcp::acceptor acceptor(
      server_context,
      tcp::endpoint(asio::ip::make_address_v4("127.0.0.1"), 0U));
  std::promise<void> accepted;
  std::future<void> accepted_future = accepted.get_future();
  std::mutex server_mutex;
  std::condition_variable_any server_wakeup;
  std::jthread server([&](std::stop_token stop_token) {
    tcp::socket socket(server_context);
    boost::system::error_code accept_error;
    acceptor.accept(socket, accept_error);
    if (accept_error) {
      return;
    }
    accepted.set_value();
    std::unique_lock<std::mutex> lock(server_mutex);
    server_wakeup.wait(lock, stop_token, [] { return false; });
  });

  bbp::FiroNodeConfig config;
  config.id = "silent-server-test";
  config.rpc_host = "127.0.0.1";
  config.rpc_port = acceptor.local_endpoint().port();
  config.rpc_user = "user";
  config.rpc_password = "password";

  bbp::FiroDriver driver(std::chrono::seconds(30));
  std::stop_source stop_source;
  std::atomic<bool> cancelled = false;
  std::exception_ptr failure;
  std::thread waiter([&] {
    try {
      driver.WaitReady(config, std::chrono::hours(24), stop_source.get_token());
    } catch (const bbp::SimulationCancelled&) {
      cancelled = true;
    } catch (...) {
      failure = std::current_exception();
    }
  });

  const bool connected = accepted_future.wait_for(std::chrono::seconds(1)) ==
                         std::future_status::ready;
  const auto stop_started = std::chrono::steady_clock::now();
  stop_source.request_stop();
  waiter.join();
  const auto stop_duration =
      std::chrono::duration_cast<std::chrono::milliseconds>(
          std::chrono::steady_clock::now() - stop_started);
  server.request_stop();
  server_wakeup.notify_all();
  if (!connected) {
    boost::system::error_code close_error;
    acceptor.close(close_error);
  }
  server.join();

  BOOST_TEST(connected);
  BOOST_TEST(!static_cast<bool>(failure));
  BOOST_TEST(cancelled.load());
  BOOST_TEST(stop_duration.count() < 500);
}
