#include "simulator_source_scenario_persistence.h"

#include <fcntl.h>
#include <unistd.h>

#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <cerrno>
#include <cstddef>
#include <exception>
#include <stdexcept>
#include <string>
#include <system_error>
#include <utility>

#include "bbp/mcp_operation_service.h"
#include "bbp/run_ownership.h"
#include "bbp/util.h"

namespace bbp::simulator_app_internal {
namespace {

class UniqueFileDescriptor {
 public:
  explicit UniqueFileDescriptor(int descriptor = -1)
      : descriptor_(descriptor) {}
  ~UniqueFileDescriptor() {
    if (descriptor_ >= 0) {
      static_cast<void>(close(descriptor_));
    }
  }

  UniqueFileDescriptor(const UniqueFileDescriptor&) = delete;
  UniqueFileDescriptor& operator=(const UniqueFileDescriptor&) = delete;
  UniqueFileDescriptor(UniqueFileDescriptor&& other) noexcept
      : descriptor_(std::exchange(other.descriptor_, -1)) {}
  UniqueFileDescriptor& operator=(UniqueFileDescriptor&& other) noexcept {
    if (this != &other) {
      if (descriptor_ >= 0) {
        static_cast<void>(close(descriptor_));
      }
      descriptor_ = std::exchange(other.descriptor_, -1);
    }
    return *this;
  }

  [[nodiscard]] int get() const { return descriptor_; }
  [[nodiscard]] bool valid() const { return descriptor_ >= 0; }

 private:
  int descriptor_ = -1;
};

std::string ExceptionMessage(const std::exception_ptr& error) {
  try {
    std::rethrow_exception(error);
  } catch (const std::exception& exception) {
    return exception.what();
  } catch (...) {
    return "unknown exception";
  }
}

}  // namespace

void WriteSourceScenarioFile(const boost::json::object& source_scenario,
                             const std::string& run_id,
                             const std::filesystem::path& run_root,
                             std::optional<int> reserved_run_root_fd) {
  boost::json::object source = source_scenario;
  source["run_id"] = run_id;
  const std::string source_text = boost::json::serialize(source) + "\n";
  if (reserved_run_root_fd) {
    CreateTextAt(*reserved_run_root_fd, "source-scenario.json", source_text);
  } else {
    WriteText(run_root / "source-scenario.json", source_text);
  }
}

boost::json::object LoadRetainedSourceScenario(
    const std::filesystem::path& source_root, std::string_view source_run_id,
    std::stop_token stop_token) {
  constexpr std::size_t kMaximumSourceScenarioBytes = 4U * 1024U * 1024U;
  boost::json::object source_scenario;
  try {
    UniqueFileDescriptor source_root_fd(
        open(source_root.c_str(),
             O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK));
    if (!source_root_fd.valid()) {
      throw std::system_error(errno, std::generic_category(),
                              "open retained replay source root");
    }
    const RunOwnership initial_ownership =
        LoadRunOwnershipAt(std::string(source_run_id), source_root,
                           source_root_fd.get(), stop_token);

    const boost::json::value parsed = boost::json::parse(
        ReadTextAt(source_root_fd.get(), "source-scenario.json",
                   kMaximumSourceScenarioBytes, stop_token));
    if (!parsed.is_object()) {
      throw std::runtime_error("the retained source scenario is not an object");
    }
    source_scenario = parsed.as_object();
    const boost::json::value* embedded_run_id =
        source_scenario.if_contains("run_id");
    if (embedded_run_id == nullptr || !embedded_run_id->is_string() ||
        embedded_run_id->as_string() != source_run_id) {
      throw std::runtime_error(
          "the retained source scenario has an inconsistent run id");
    }

    const RunOwnership final_ownership =
        LoadRunOwnershipAt(std::string(source_run_id), source_root,
                           source_root_fd.get(), stop_token);
    if (final_ownership != initial_ownership) {
      throw std::runtime_error(
          "the retained source identity changed while it was read");
    }
  } catch (const McpOperationFailure&) {
    throw;
  } catch (...) {
    if (stop_token.stop_requested()) {
      throw McpOperationCancelled();
    }
    throw McpOperationFailure(
        "run_replay_source_unavailable",
        "the retained replay source could not be verified: " +
            ExceptionMessage(std::current_exception()),
        false);
  }
  return source_scenario;
}

}  // namespace bbp::simulator_app_internal
