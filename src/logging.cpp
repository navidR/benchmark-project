#include "bbp/logging.h"

#include <algorithm>
#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sinks/sync_frontend.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sinks/text_ostream_backend.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <ios>
#include <mutex>
#include <optional>
#include <stdexcept>

#include "bbp/log_view.h"

namespace bbp {
namespace {

using TextFileSink =
    boost::log::sinks::synchronous_sink<boost::log::sinks::text_file_backend>;
using ConsoleSink = boost::log::sinks::synchronous_sink<
    boost::log::sinks::text_ostream_backend>;

std::once_flag init_logging_once;
std::mutex console_log_mutex;
std::timed_mutex run_log_mutex;
boost::shared_ptr<ConsoleSink> console_log_sink;
boost::shared_ptr<TextFileSink> run_log_sink;
std::optional<std::filesystem::path> attached_run_log_root;
bool console_logging_enabled = false;

auto LogFormatter() {
  namespace expr = boost::log::expressions;
  return expr::stream << expr::format_date_time<boost::posix_time::ptime>(
                             "TimeStamp", "%Y-%m-%dT%H:%M:%SZ")
                      << " [" << boost::log::trivial::severity << "] "
                      << expr::smessage;
}

boost::log::trivial::severity_level BoostSeverity(LogLevel level) {
  switch (level) {
    case LogLevel::kTrace:
      return boost::log::trivial::trace;
    case LogLevel::kDebug:
      return boost::log::trivial::debug;
    case LogLevel::kInfo:
      return boost::log::trivial::info;
    case LogLevel::kWarning:
      return boost::log::trivial::warning;
    case LogLevel::kError:
      return boost::log::trivial::error;
    case LogLevel::kFatal:
      return boost::log::trivial::fatal;
  }
  throw std::logic_error("unknown log level");
}

}  // namespace

void InitLogging() {
  std::call_once(init_logging_once, [] {
    boost::log::add_common_attributes();
    auto sink = boost::log::add_console_log();
    sink->set_formatter(LogFormatter());
    console_log_sink = std::move(sink);
    console_logging_enabled = true;
  });
}

void SetMinimumLogLevel(LogLevel level) {
  InitLogging();
  boost::log::core::get()->set_filter(boost::log::trivial::severity >=
                                      BoostSeverity(level));
}

void SetConsoleLoggingEnabled(bool enabled) {
  InitLogging();
  std::lock_guard<std::mutex> lock(console_log_mutex);
  if (enabled == console_logging_enabled) {
    return;
  }
  if (enabled) {
    boost::log::core::get()->add_sink(console_log_sink);
  } else {
    boost::log::core::get()->remove_sink(console_log_sink);
  }
  console_logging_enabled = enabled;
}

void AttachRunLogFile(const std::filesystem::path& run_root) {
  InitLogging();
  const std::filesystem::path absolute_run_root =
      std::filesystem::absolute(run_root).lexically_normal();
  const std::filesystem::path log_path = RunLogPath(absolute_run_root);
  std::lock_guard<std::timed_mutex> lock(run_log_mutex);
  if (run_log_sink != nullptr) {
    run_log_sink->flush();
    boost::log::core::get()->remove_sink(run_log_sink);
    run_log_sink.reset();
    attached_run_log_root.reset();
  }
  run_log_sink = boost::log::add_file_log(
      boost::log::keywords::file_name = log_path.string(),
      boost::log::keywords::open_mode = static_cast<std::ios_base::openmode>(
          std::ios_base::out | std::ios_base::app),
      boost::log::keywords::auto_flush = true,
      boost::log::keywords::format = LogFormatter());
  attached_run_log_root = absolute_run_root;
}

void DetachRunLogFile(
    const std::filesystem::path& run_root,
    std::optional<std::chrono::steady_clock::time_point> deadline,
    std::stop_token stop_token) {
  const auto require_active = [&] {
    if (stop_token.stop_requested()) {
      throw std::runtime_error("run log detachment was cancelled");
    }
    if (deadline && std::chrono::steady_clock::now() >= *deadline) {
      throw std::runtime_error("run log detachment deadline expired");
    }
  };
  require_active();
  InitLogging();
  const std::filesystem::path absolute_run_root =
      std::filesystem::absolute(run_root).lexically_normal();
  std::unique_lock<std::timed_mutex> lock(run_log_mutex, std::defer_lock);
  constexpr auto kLockPollInterval = std::chrono::milliseconds(10);
  while (!lock.try_lock_until(
      deadline ? std::min(*deadline,
                          std::chrono::steady_clock::now() + kLockPollInterval)
               : std::chrono::steady_clock::now() + kLockPollInterval)) {
    require_active();
  }
  require_active();
  if (run_log_sink == nullptr || !attached_run_log_root ||
      *attached_run_log_root != absolute_run_root) {
    return;
  }
  run_log_sink->flush();
  require_active();
  boost::log::core::get()->remove_sink(run_log_sink);
  run_log_sink.reset();
  attached_run_log_root.reset();
}

}  // namespace bbp
