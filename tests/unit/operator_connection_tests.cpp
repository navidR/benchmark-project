#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <future>
#include <iomanip>
#include <sstream>
#include <stop_token>
#include <string>
#include <utility>
#include <vector>

#include "bbp/operator_connection.h"
#include "bbp/simulation_cancelled.h"
#ifdef BBP_FIRO_GUI_LAUNCHER
#include "bbp/drivers/firo_gui_launcher.h"
#endif

#ifdef BBP_FIRO_GUI_LAUNCHER
namespace {

class ScopedFileRemoval {
 public:
  explicit ScopedFileRemoval(std::filesystem::path path)
      : path_(std::move(path)) {}
  ~ScopedFileRemoval() {
    std::error_code error;
    std::filesystem::remove(path_, error);
  }

 private:
  std::filesystem::path path_;
};

class ScopedDescriptor {
 public:
  explicit ScopedDescriptor(int descriptor = -1) : descriptor_(descriptor) {}
  ~ScopedDescriptor() { Reset(); }

  ScopedDescriptor(const ScopedDescriptor&) = delete;
  ScopedDescriptor& operator=(const ScopedDescriptor&) = delete;

  void Reset(int descriptor = -1) noexcept {
    if (descriptor_ >= 0) {
      static_cast<void>(close(descriptor_));
    }
    descriptor_ = descriptor;
  }

  [[nodiscard]] int get() const noexcept { return descriptor_; }
  [[nodiscard]] int Release() noexcept {
    return std::exchange(descriptor_, -1);
  }

 private:
  int descriptor_ = -1;
};

class ScopedLauncherCleanupHook {
 public:
  explicit ScopedLauncherCleanupHook(bbp::FiroQtLauncherCleanupTestHook hook) {
    bbp::SetFiroQtLauncherCleanupTestHook(std::move(hook));
  }
  ~ScopedLauncherCleanupHook() { bbp::SetFiroQtLauncherCleanupTestHook({}); }

  ScopedLauncherCleanupHook(const ScopedLauncherCleanupHook&) = delete;
  ScopedLauncherCleanupHook& operator=(const ScopedLauncherCleanupHook&) =
      delete;
};

class AtomicReleaseGuard {
 public:
  explicit AtomicReleaseGuard(std::atomic_bool* release) : release_(release) {}
  ~AtomicReleaseGuard() {
    release_->store(true, std::memory_order_release);
    release_->notify_all();
  }

  AtomicReleaseGuard(const AtomicReleaseGuard&) = delete;
  AtomicReleaseGuard& operator=(const AtomicReleaseGuard&) = delete;

 private:
  std::atomic_bool* release_;
};

std::string ReadFile(const std::filesystem::path& path) {
  std::ifstream stream(path, std::ios::binary);
  return std::string(std::istreambuf_iterator<char>(stream),
                     std::istreambuf_iterator<char>());
}

void WriteAll(int descriptor, std::string_view content) {
  std::size_t offset = 0U;
  while (offset < content.size()) {
    const ssize_t count =
        write(descriptor, content.data() + offset, content.size() - offset);
    if (count > 0) {
      offset += static_cast<std::size_t>(count);
    } else if (count < 0 && errno == EINTR) {
      continue;
    } else {
      throw std::system_error(errno, std::generic_category(),
                              "write launcher test file");
    }
  }
}

void WriteNewFile(const std::filesystem::path& path, std::string_view content) {
  const int descriptor =
      open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC, 0600);
  if (descriptor < 0) {
    throw std::system_error(errno, std::generic_category(),
                            "create launcher test file");
  }
  try {
    WriteAll(descriptor, content);
  } catch (...) {
    static_cast<void>(close(descriptor));
    throw;
  }
  if (close(descriptor) != 0) {
    throw std::system_error(errno, std::generic_category(),
                            "close launcher test file");
  }
}

std::filesystem::path CreateMatchingForeignLauncher() {
  for (unsigned long attempt = 0UL; attempt < 100UL; ++attempt) {
    std::ostringstream suffix;
    suffix << std::setw(6) << std::setfill('0')
           << ((static_cast<unsigned long>(getpid()) + attempt) % 1000000UL);
    const std::filesystem::path path =
        "/tmp/bbp-firo-qt-" + suffix.str() + ".sh";
    try {
      WriteNewFile(path, "foreign launcher\n");
      return path;
    } catch (const std::system_error& error) {
      if (error.code().value() != EEXIST) {
        throw;
      }
    }
  }
  throw std::runtime_error("could not reserve a foreign launcher collision");
}

bbp::OperatorConnectionCommand LauncherConnection(std::uint16_t peer_port) {
  bbp::OperatorConnectionCommand connection;
  connection.executable = "/opt/firo/firo-qt";
  connection.data_dir = "/tmp/bbp-operator/firo-qt";
  connection.peer_address = "127.0.0.1";
  connection.peer_port = peer_port;
  const std::string peer_endpoint =
      connection.peer_address + ":" + std::to_string(peer_port);
  connection.arguments = {
      "-regtest",
      "-datadir=" + connection.data_dir.string(),
      "-connect=" + peer_endpoint,
      "-dns=0",
      "-dnsseed=0",
      "-forcednsseed=0",
      "-maxconnections=1",
      "-listen=0",
      "-discover=0",
      "-listenonion=0",
      "-torsetup=0",
      "-upnp=0",
  };
  return connection;
}

void SetLauncherConnection(boost::json::object* report,
                           const bbp::OperatorConnectionCommand& connection) {
  boost::json::array arguments;
  boost::json::array argv{connection.executable.string()};
  for (const std::string& argument : connection.arguments) {
    arguments.emplace_back(argument);
    argv.emplace_back(argument);
  }
  boost::json::object& command =
      report->at("operator_connection_command").as_object();
  command["executable"] = connection.executable.string();
  command["arguments"] = std::move(arguments);
  command["argv"] = std::move(argv);
  command["command"] = connection.ShellCommand();
  command["data_dir"] = connection.data_dir.string();
  command["peer_address"] = connection.peer_address;
  command["peer_port"] = connection.peer_port;
  command["peer_endpoint"] =
      connection.peer_address + ":" + std::to_string(connection.peer_port);
}

boost::json::object LauncherReport(std::string_view node_id,
                                   std::uint16_t peer_port) {
  const bbp::OperatorConnectionCommand connection =
      LauncherConnection(peer_port);
  boost::json::array arguments;
  boost::json::array argv{connection.executable.string()};
  for (const std::string& argument : connection.arguments) {
    arguments.emplace_back(argument);
    argv.emplace_back(argument);
  }
  const std::string peer_endpoint =
      connection.peer_address + ":" + std::to_string(peer_port);
  return boost::json::object{
      {"chain", "firo"},
      {"operator_connection_command",
       boost::json::object{
           {"node_id", node_id},
           {"timestamp", "2026-07-27T00:00:00Z"},
           {"kind", "manual_firo_gui"},
           {"manual_launch", true},
           {"discovery_disabled", true},
           {"wallet_enabled", true},
           {"network", "regtest"},
           {"executable", connection.executable.string()},
           {"arguments", std::move(arguments)},
           {"argv", std::move(argv)},
           {"command", connection.ShellCommand()},
           {"data_dir", connection.data_dir.string()},
           {"peer_address", connection.peer_address},
           {"peer_port", connection.peer_port},
           {"peer_endpoint", peer_endpoint},
       }}};
}

}  // namespace
#endif

BOOST_AUTO_TEST_CASE(posix_shell_quote_handles_hostile_arguments) {
  BOOST_TEST(bbp::PosixShellQuote("") == "''");
  BOOST_TEST(bbp::PosixShellQuote("plain") == "'plain'");
  BOOST_TEST(bbp::PosixShellQuote("two words") == "'two words'");
  BOOST_TEST(bbp::PosixShellQuote("a'b") == "'a'\"'\"'b'");
  BOOST_TEST(bbp::PosixShellQuote("$HOME;$(id);`id`\nnext") ==
             "'$HOME;$(id);`id`\nnext'");
  const std::string nul_argument("safe\0unsafe", 11U);
  BOOST_CHECK_THROW(bbp::PosixShellQuote(nul_argument), std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(operator_connection_renders_every_argv_element) {
  bbp::OperatorConnectionCommand connection;
  connection.executable = "/tmp/Firo GUI/firo-qt";
  connection.arguments = {"-regtest", "-datadir=/tmp/a'b", "$(touch /tmp/x)"};
  BOOST_TEST(connection.ShellCommand() ==
             "'/tmp/Firo GUI/firo-qt' '-regtest' "
             "'-datadir=/tmp/a'\"'\"'b' '$(touch /tmp/x)'");

  connection.executable.clear();
  BOOST_CHECK_THROW(static_cast<void>(connection.ShellCommand()),
                    std::invalid_argument);
}

BOOST_AUTO_TEST_CASE(operator_connection_is_recovered_from_run_report) {
  boost::json::object report;
  BOOST_TEST(bbp::OperatorConnectionCommandFromReport(report).empty());
  report["operator_connection_command"] = nullptr;
  BOOST_TEST(bbp::OperatorConnectionCommandFromReport(report).empty());
  report["operator_connection_command"] = boost::json::object{{"command", 1}};
  BOOST_TEST(bbp::OperatorConnectionCommandFromReport(report).empty());
  report["operator_connection_command"] =
      boost::json::object{{"command", "'/opt/firo/firo-qt' '-regtest'"}};
  BOOST_TEST(bbp::OperatorConnectionCommandFromReport(report) ==
             "'/opt/firo/firo-qt' '-regtest'");
}

#ifdef BBP_FIRO_GUI_LAUNCHER
BOOST_AUTO_TEST_CASE(
    firo_qt_launcher_has_exact_content_mode_execution_and_cleanup) {
  const std::filesystem::path foreign_path = CreateMatchingForeignLauncher();
  ScopedFileRemoval remove_foreign(foreign_path);

  const std::filesystem::path execution_marker =
      "/tmp/bbp-firo-qt-executed-" + std::to_string(getpid());
  ScopedFileRemoval remove_execution_marker(execution_marker);
  const std::string marker_command =
      "printf 'launcher executed\\n' > " +
      bbp::PosixShellQuote(execution_marker.string());
  const std::string command =
      "'/bin/sh' '-c' " + bbp::PosixShellQuote(marker_command);
  bbp::OwnedFiroQtLauncher launcher = bbp::OwnedFiroQtLauncher::Create(command);
  const std::filesystem::path launcher_path = launcher.path();

  BOOST_TEST(launcher.active());
  BOOST_TEST(launcher_path.parent_path() ==
             std::filesystem::path("/proc") / std::to_string(getpid()) / "fd");
  BOOST_TEST(launcher_path != foreign_path);
  BOOST_TEST(ReadFile(foreign_path) == "foreign launcher\n");
  BOOST_TEST(ReadFile(launcher_path) == "#!/bin/bash\nexec " + command + "\n");
  BOOST_TEST(!std::filesystem::exists(execution_marker));

  struct stat status{};
  BOOST_REQUIRE(stat(launcher_path.c_str(), &status) == 0);
  BOOST_CHECK(S_ISREG(status.st_mode));
  BOOST_TEST((status.st_mode & 07777) == S_IRWXU);

  const pid_t launcher_process = fork();
  if (launcher_process == 0) {
    execl(launcher_path.c_str(), launcher_path.c_str(),
          static_cast<char*>(nullptr));
    _exit(127);
  }
  BOOST_REQUIRE(launcher_process > 0);
  int launcher_status = 0;
  pid_t waited = -1;
  do {
    waited = waitpid(launcher_process, &launcher_status, 0);
  } while (waited < 0 && errno == EINTR);
  BOOST_REQUIRE(waited == launcher_process);
  BOOST_REQUIRE(WIFEXITED(launcher_status));
  BOOST_TEST(WEXITSTATUS(launcher_status) == 0);
  BOOST_TEST(ReadFile(execution_marker) == "launcher executed\n");

  BOOST_CHECK(launcher.Cleanup() == bbp::FiroQtLauncherCleanupResult::kRemoved);
  BOOST_TEST(!launcher.active());
  errno = 0;
  BOOST_TEST(lstat(launcher_path.c_str(), &status) == -1);
  BOOST_TEST(errno == ENOENT);
  BOOST_TEST(ReadFile(foreign_path) == "foreign launcher\n");
}

BOOST_AUTO_TEST_CASE(
    firo_qt_launcher_final_release_rejects_foreign_path_installation) {
  bbp::OwnedFiroQtLauncher launcher =
      bbp::OwnedFiroQtLauncher::Create("'/opt/firo/firo-qt' '-regtest'");
  const std::filesystem::path launcher_path = launcher.path();
  const std::filesystem::path foreign_path = CreateMatchingForeignLauncher();
  ScopedFileRemoval remove_foreign(foreign_path);
  struct stat foreign_status{};
  BOOST_REQUIRE(lstat(foreign_path.c_str(), &foreign_status) == 0);
  bool attempted_installation = false;
  std::error_code installation_error;
  ScopedLauncherCleanupHook hook(
      [&](bbp::FiroQtLauncherCleanupTestPhase phase,
          const std::filesystem::path& descriptor_path,
          const std::optional<std::filesystem::path>&) {
        if (phase !=
            bbp::FiroQtLauncherCleanupTestPhase::kBeforeDescriptorRelease) {
          return;
        }
        attempted_installation = true;
        BOOST_TEST(descriptor_path == launcher_path);
        std::filesystem::rename(foreign_path, launcher_path,
                                installation_error);
      });

  BOOST_CHECK(launcher.Cleanup() == bbp::FiroQtLauncherCleanupResult::kRemoved);
  BOOST_TEST(attempted_installation);
  BOOST_TEST(installation_error.value() != 0);
  BOOST_TEST(!std::filesystem::exists(launcher_path));
  struct stat retained_status{};
  BOOST_REQUIRE(lstat(foreign_path.c_str(), &retained_status) == 0);
  BOOST_TEST(retained_status.st_dev == foreign_status.st_dev);
  BOOST_TEST(retained_status.st_ino == foreign_status.st_ino);
  BOOST_TEST(ReadFile(foreign_path) == "foreign launcher\n");
}

BOOST_AUTO_TEST_CASE(firo_qt_launcher_destructor_removes_exact_owned_file) {
  std::filesystem::path launcher_path;
  {
    bbp::OwnedFiroQtLauncher launcher =
        bbp::OwnedFiroQtLauncher::Create("'/opt/firo/firo-qt' '-regtest'");
    launcher_path = launcher.path();
    BOOST_REQUIRE(std::filesystem::exists(launcher_path));
  }
  struct stat status{};
  errno = 0;
  BOOST_TEST(lstat(launcher_path.c_str(), &status) == -1);
  BOOST_TEST(errno == ENOENT);
}

BOOST_AUTO_TEST_CASE(
    firo_qt_launcher_service_binds_node_replaces_and_honors_cancellation) {
  const std::string first_command = LauncherConnection(18444U).ShellCommand();
  const std::string second_command = LauncherConnection(18445U).ShellCommand();
  std::uint64_t inventory_generation = 1U;
  bbp::OperatorConnectionCommand authoritative_connection =
      LauncherConnection(18444U);
  bbp::FiroQtLauncherService service([&](std::string_view, std::stop_token) {
    return bbp::FiroQtLauncherAuthority{
        .inventory_generation = inventory_generation,
        .node_id = "firo-1",
        .command = authoritative_connection,
    };
  });

  BOOST_CHECK_THROW(
      service.ReplaceFromReport(LauncherReport("firo-1", 18444U), "firo-2"),
      std::runtime_error);
  BOOST_CHECK_THROW(
      service.ReplaceFromReport(LauncherReport("firo.1", 18444U), "firo.1"),
      std::runtime_error);
  boost::json::object unsafe_report = LauncherReport("firo-1", 18444U);
  unsafe_report.at("operator_connection_command").as_object()["bearer_token"] =
      "must-not-be-copied";
  BOOST_CHECK_THROW(service.ReplaceFromReport(unsafe_report, "firo-1"),
                    std::runtime_error);
  boost::json::object oversized_report = LauncherReport("firo-1", 18444U);
  bbp::OperatorConnectionCommand oversized_connection =
      LauncherConnection(18444U);
  oversized_connection.data_dir =
      "/tmp/" + std::string(1024U * 1024U + 1U, 'x');
  oversized_connection.arguments[1U] =
      "-datadir=" + oversized_connection.data_dir.string();
  SetLauncherConnection(&oversized_report, oversized_connection);
  authoritative_connection = oversized_connection;
  BOOST_CHECK_THROW(service.ReplaceFromReport(oversized_report, "firo-1"),
                    std::runtime_error);
  authoritative_connection = LauncherConnection(18444U);
  boost::json::object foreign_data_report = LauncherReport("firo-1", 18444U);
  bbp::OperatorConnectionCommand foreign_data_connection =
      LauncherConnection(18444U);
  foreign_data_connection.data_dir = "/tmp/foreign-operator/firo-qt";
  foreign_data_connection.arguments[1U] =
      "-datadir=" + foreign_data_connection.data_dir.string();
  SetLauncherConnection(&foreign_data_report, foreign_data_connection);
  BOOST_CHECK_THROW(service.ReplaceFromReport(foreign_data_report, "firo-1"),
                    std::runtime_error);
  boost::json::object foreign_executable_report =
      LauncherReport("firo-1", 18444U);
  bbp::OperatorConnectionCommand foreign_executable_connection =
      LauncherConnection(18444U);
  foreign_executable_connection.executable = "/tmp/foreign/firo-qt";
  SetLauncherConnection(&foreign_executable_report,
                        foreign_executable_connection);
  BOOST_CHECK_THROW(
      service.ReplaceFromReport(foreign_executable_report, "firo-1"),
      std::runtime_error);
  boost::json::object control_report = LauncherReport("firo-1", 18444U);
  bbp::OperatorConnectionCommand control_connection =
      LauncherConnection(18444U);
  control_connection.data_dir = "/tmp/bad";
  control_connection.data_dir += std::string(1U, '\x01');
  control_connection.arguments[1U] =
      "-datadir=" + control_connection.data_dir.string();
  SetLauncherConnection(&control_report, control_connection);
  authoritative_connection = control_connection;
  BOOST_CHECK_THROW(service.ReplaceFromReport(control_report, "firo-1"),
                    std::runtime_error);
  authoritative_connection = LauncherConnection(18444U);
  BOOST_TEST(!service.Snapshot().has_value());

  const bbp::FiroQtLauncherSnapshot first =
      service.ReplaceFromReport(LauncherReport("firo-1", 18444U), "firo-1");
  BOOST_TEST(first.node_id == "firo-1");
  BOOST_TEST(first.operator_command == first_command);
  BOOST_TEST(std::filesystem::exists(first.launcher_path));

  std::stop_source cancellation;
  cancellation.request_stop();
  BOOST_CHECK_THROW(
      service.ReplaceFromReport(LauncherReport("firo-1", 18445U), "firo-1",
                                cancellation.get_token()),
      bbp::SimulationCancelled);
  const std::optional<bbp::FiroQtLauncherSnapshot> after_cancellation =
      service.Snapshot();
  BOOST_REQUIRE(after_cancellation);
  BOOST_TEST(after_cancellation->launcher_path == first.launcher_path);
  BOOST_TEST(std::filesystem::exists(first.launcher_path));

  ++inventory_generation;
  authoritative_connection = LauncherConnection(18445U);
  std::stop_source cleanup_cancellation;
  std::vector<std::filesystem::path> cleanup_paths;
  {
    ScopedLauncherCleanupHook hook(
        [&](bbp::FiroQtLauncherCleanupTestPhase phase,
            const std::filesystem::path& public_path,
            const std::optional<std::filesystem::path>&) {
          if (phase !=
              bbp::FiroQtLauncherCleanupTestPhase::kBeforeDescriptorRelease) {
            return;
          }
          cleanup_paths.push_back(public_path);
          if (cleanup_paths.size() == 1U) {
            cleanup_cancellation.request_stop();
          }
        });
    BOOST_CHECK_THROW(
        service.ReplaceFromReport(LauncherReport("firo-1", 18445U), "firo-1",
                                  cleanup_cancellation.get_token()),
        bbp::SimulationCancelled);
  }
  BOOST_REQUIRE(cleanup_paths.size() == 2U);
  BOOST_TEST(!std::filesystem::exists(cleanup_paths[0U]));
  BOOST_TEST(!std::filesystem::exists(cleanup_paths[1U]));
  BOOST_TEST(!service.Snapshot().has_value());

  const bbp::FiroQtLauncherSnapshot second =
      service.ReplaceFromReport(LauncherReport("firo-1", 18445U), "firo-1");
  BOOST_TEST(std::filesystem::exists(second.launcher_path));
  BOOST_TEST(second.operator_command == second_command);
  BOOST_CHECK(service.CloseAndCleanup() ==
              bbp::FiroQtLauncherCleanupResult::kRemoved);
  BOOST_TEST(!std::filesystem::exists(second.launcher_path));
  BOOST_CHECK_THROW(
      service.ReplaceFromReport(LauncherReport("firo-1", 18444U), "firo-1"),
      std::runtime_error);
}

BOOST_AUTO_TEST_CASE(
    firo_qt_launcher_service_replacement_retains_lost_descriptor_uncertainty) {
  std::uint64_t inventory_generation = 1U;
  std::uint16_t authoritative_port = 18444U;
  std::size_t authority_resolutions = 0U;
  bbp::FiroQtLauncherService service([&](std::string_view, std::stop_token) {
    ++authority_resolutions;
    return bbp::FiroQtLauncherAuthority{
        .inventory_generation = inventory_generation,
        .node_id = "firo-1",
        .command = LauncherConnection(authoritative_port),
    };
  });
  const bbp::FiroQtLauncherSnapshot first =
      service.ReplaceFromReport(LauncherReport("firo-1", 18444U), "firo-1");
  ++inventory_generation;
  authoritative_port = 18445U;
  bool descriptor_closed = false;
  ScopedDescriptor foreign_descriptor;
  {
    ScopedLauncherCleanupHook hook(
        [&](bbp::FiroQtLauncherCleanupTestPhase phase,
            const std::filesystem::path& descriptor_path,
            const std::optional<std::filesystem::path>&) {
          if (phase != bbp::FiroQtLauncherCleanupTestPhase::
                           kBeforeDescriptorRelease ||
              descriptor_path != first.launcher_path || descriptor_closed) {
            return;
          }
          const int descriptor = std::stoi(descriptor_path.filename().string());
          BOOST_REQUIRE(close(descriptor) == 0);
          ScopedDescriptor source(open("/dev/null", O_RDONLY | O_CLOEXEC));
          BOOST_REQUIRE(source.get() >= 0);
          if (source.get() == descriptor) {
            foreign_descriptor.Reset(source.Release());
          } else {
            ScopedDescriptor reused(dup3(source.get(), descriptor, O_CLOEXEC));
            BOOST_REQUIRE(reused.get() == descriptor);
            foreign_descriptor.Reset(reused.Release());
          }
          descriptor_closed = true;
        });
    BOOST_CHECK_THROW(
        service.ReplaceFromReport(LauncherReport("firo-1", 18445U), "firo-1"),
        bbp::FiroQtLauncherCleanupUnverified);
  }

  BOOST_TEST(descriptor_closed);
  BOOST_REQUIRE(foreign_descriptor.get() >= 0);
  BOOST_TEST(fcntl(foreign_descriptor.get(), F_GETFD) >= 0);
  foreign_descriptor.Reset();
  BOOST_TEST(!service.Snapshot().has_value());
  BOOST_TEST(!std::filesystem::exists(first.launcher_path));

  const std::size_t resolutions_after_failure = authority_resolutions;
  BOOST_CHECK_THROW(
      service.ReplaceFromReport(LauncherReport("firo-1", 18445U), "firo-1"),
      bbp::FiroQtLauncherCleanupUnverified);
  BOOST_TEST(authority_resolutions == resolutions_after_failure);
  BOOST_TEST(!service.Snapshot().has_value());
}

BOOST_AUTO_TEST_CASE(
    firo_qt_launcher_service_close_attempts_pending_and_published_cleanup) {
  std::uint64_t inventory_generation = 1U;
  std::uint16_t authoritative_port = 18444U;
  bbp::FiroQtLauncherService service([&](std::string_view, std::stop_token) {
    return bbp::FiroQtLauncherAuthority{
        .inventory_generation = inventory_generation,
        .node_id = "firo-1",
        .command = LauncherConnection(authoritative_port),
    };
  });
  const bbp::FiroQtLauncherSnapshot first =
      service.ReplaceFromReport(LauncherReport("firo-1", 18444U), "firo-1");
  ++inventory_generation;
  authoritative_port = 18445U;

  std::vector<std::filesystem::path> failed_cleanup_paths;
  {
    ScopedLauncherCleanupHook hook(
        [&](bbp::FiroQtLauncherCleanupTestPhase phase,
            const std::filesystem::path& public_path,
            const std::optional<std::filesystem::path>&) {
          if (phase !=
              bbp::FiroQtLauncherCleanupTestPhase::kBeforeDescriptorRelease) {
            return;
          }
          failed_cleanup_paths.push_back(public_path);
          throw std::runtime_error("injected retained cleanup failure");
        });
    BOOST_CHECK_THROW(
        service.ReplaceFromReport(LauncherReport("firo-1", 18445U), "firo-1"),
        std::runtime_error);
  }
  BOOST_REQUIRE(failed_cleanup_paths.size() == 2U);
  const std::filesystem::path candidate_path = failed_cleanup_paths[1U];
  BOOST_TEST(failed_cleanup_paths[0U] == first.launcher_path);
  BOOST_TEST(std::filesystem::exists(first.launcher_path));
  BOOST_TEST(std::filesystem::exists(candidate_path));
  BOOST_REQUIRE(service.Snapshot().has_value());

  {
    ScopedLauncherCleanupHook hook(
        [&](bbp::FiroQtLauncherCleanupTestPhase phase,
            const std::filesystem::path& public_path,
            const std::optional<std::filesystem::path>&) {
          if (phase == bbp::FiroQtLauncherCleanupTestPhase::
                           kBeforeDescriptorRelease &&
              public_path == candidate_path) {
            throw std::runtime_error("injected pending cleanup retry failure");
          }
        });
    BOOST_CHECK_THROW(service.CloseAndCleanup(), std::runtime_error);
  }
  BOOST_TEST(!std::filesystem::exists(first.launcher_path));
  BOOST_TEST(std::filesystem::exists(candidate_path));
  BOOST_TEST(!service.Snapshot().has_value());
  BOOST_CHECK(service.CloseAndCleanup() ==
              bbp::FiroQtLauncherCleanupResult::kRemoved);
  BOOST_TEST(!std::filesystem::exists(candidate_path));
}

BOOST_AUTO_TEST_CASE(
    firo_qt_launcher_service_close_waits_while_snapshot_remains_prompt) {
  using namespace std::chrono_literals;
  std::uint64_t inventory_generation = 1U;
  std::uint16_t authoritative_port = 18444U;
  bbp::FiroQtLauncherService service([&](std::string_view, std::stop_token) {
    return bbp::FiroQtLauncherAuthority{
        .inventory_generation = inventory_generation,
        .node_id = "firo-1",
        .command = LauncherConnection(authoritative_port),
    };
  });
  const bbp::FiroQtLauncherSnapshot first =
      service.ReplaceFromReport(LauncherReport("firo-1", 18444U), "firo-1");
  ++inventory_generation;
  authoritative_port = 18445U;
  std::promise<void> cleanup_entered;
  std::future<void> cleanup_entered_future = cleanup_entered.get_future();
  std::atomic_bool entered = false;
  std::atomic_bool release = false;
  ScopedLauncherCleanupHook hook(
      [&](bbp::FiroQtLauncherCleanupTestPhase phase,
          const std::filesystem::path&,
          const std::optional<std::filesystem::path>&) {
        if (phase !=
            bbp::FiroQtLauncherCleanupTestPhase::kBeforeDescriptorRelease) {
          return;
        }
        if (!entered.exchange(true, std::memory_order_acq_rel)) {
          cleanup_entered.set_value();
        }
        release.wait(false, std::memory_order_acquire);
      });

  std::future<bbp::FiroQtLauncherSnapshot> replacement =
      std::async(std::launch::async, [&] {
        return service.ReplaceFromReport(LauncherReport("firo-1", 18445U),
                                         "firo-1");
      });
  AtomicReleaseGuard release_on_failure(&release);
  BOOST_REQUIRE(cleanup_entered_future.wait_for(2s) ==
                std::future_status::ready);

  std::future<std::optional<bbp::FiroQtLauncherSnapshot>> snapshot =
      std::async(std::launch::async, [&] { return service.Snapshot(); });
  BOOST_REQUIRE(snapshot.wait_for(200ms) == std::future_status::ready);
  const std::optional<bbp::FiroQtLauncherSnapshot> during_replacement =
      snapshot.get();
  BOOST_REQUIRE(during_replacement);
  BOOST_TEST(during_replacement->launcher_path == first.launcher_path);

  BOOST_CHECK_THROW(
      service.CloseAndCleanup(std::chrono::steady_clock::now() + 50ms),
      std::runtime_error);
  BOOST_REQUIRE(service.Snapshot().has_value());

  std::future<bbp::FiroQtLauncherCleanupResult> close =
      std::async(std::launch::async, [&] { return service.CloseAndCleanup(); });
  BOOST_CHECK(close.wait_for(100ms) == std::future_status::timeout);
  release.store(true, std::memory_order_release);
  release.notify_all();

  const bbp::FiroQtLauncherSnapshot second = replacement.get();
  BOOST_CHECK(close.get() == bbp::FiroQtLauncherCleanupResult::kRemoved);
  BOOST_TEST(!std::filesystem::exists(first.launcher_path));
  BOOST_TEST(!std::filesystem::exists(second.launcher_path));
  BOOST_TEST(!service.Snapshot().has_value());
}

BOOST_AUTO_TEST_CASE(
    firo_qt_launcher_service_close_cancellation_preserves_retryable_descriptor) {
  using namespace std::chrono_literals;
  bbp::FiroQtLauncherService service([](std::string_view, std::stop_token) {
    return bbp::FiroQtLauncherAuthority{
        .inventory_generation = 1U,
        .node_id = "firo-1",
        .command = LauncherConnection(18444U),
    };
  });
  const bbp::FiroQtLauncherSnapshot launcher =
      service.ReplaceFromReport(LauncherReport("firo-1", 18444U), "firo-1");
  std::stop_source cancellation;
  bool release_reached = false;
  {
    ScopedLauncherCleanupHook hook(
        [&](bbp::FiroQtLauncherCleanupTestPhase phase,
            const std::filesystem::path&,
            const std::optional<std::filesystem::path>&) {
          if (phase !=
              bbp::FiroQtLauncherCleanupTestPhase::kBeforeDescriptorRelease) {
            return;
          }
          release_reached = true;
          cancellation.request_stop();
        });
    BOOST_CHECK_THROW(service.CloseAndCleanup(cancellation.get_token()),
                      bbp::SimulationCancelled);
  }

  BOOST_TEST(release_reached);
  BOOST_TEST(std::filesystem::exists(launcher.launcher_path));
  BOOST_REQUIRE(service.Snapshot().has_value());
  BOOST_CHECK(service.CloseAndCleanup() ==
              bbp::FiroQtLauncherCleanupResult::kRemoved);
  BOOST_TEST(!std::filesystem::exists(launcher.launcher_path));
  BOOST_TEST(!service.Snapshot().has_value());
}

#endif
