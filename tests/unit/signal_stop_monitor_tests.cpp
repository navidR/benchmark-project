#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <chrono>
#include <thread>

#include "bbp/signal_stop_monitor.h"

BOOST_AUTO_TEST_CASE(signal_stop_monitor_requests_stop_for_sigint) {
  const pid_t child = fork();
  BOOST_REQUIRE(child >= 0);
  if (child == 0) {
    int result = 0;
    try {
      {
        bbp::SignalStopMonitor monitor;
        if (kill(getpid(), SIGINT) != 0) {
          result = 2;
        }
        const auto deadline =
            std::chrono::steady_clock::now() + std::chrono::seconds(2);
        while (result == 0 && !monitor.GetToken().stop_requested() &&
               std::chrono::steady_clock::now() < deadline) {
          std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        if (result == 0 && (!monitor.GetToken().stop_requested() ||
                            monitor.ReceivedSignal() != SIGINT)) {
          result = 3;
        }
      }
    } catch (...) {
      result = 4;
    }
    _exit(result);
  }

  int status = 0;
  BOOST_REQUIRE(waitpid(child, &status, 0) == child);
  BOOST_REQUIRE(WIFEXITED(status));
  BOOST_TEST(WEXITSTATUS(status) == 0);
}
