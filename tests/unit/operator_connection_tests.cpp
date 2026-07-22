#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <boost/json/object.hpp>
#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <string>

#include "bbp/operator_connection.h"

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

}  // namespace

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
  BOOST_CHECK_THROW(connection.ShellCommand(), std::invalid_argument);
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

BOOST_AUTO_TEST_CASE(firo_qt_launcher_has_exact_content_mode_and_cleanup) {
  const std::filesystem::path foreign_path = CreateMatchingForeignLauncher();
  ScopedFileRemoval remove_foreign(foreign_path);

  const std::filesystem::path execution_marker =
      "/tmp/bbp-firo-qt-never-run-" + std::to_string(getpid());
  ScopedFileRemoval remove_execution_marker(execution_marker);
  const std::string command =
      "'/tmp/Firo GUI/firo-qt' '-regtest' "
      "'-datadir=/tmp/operator/a'\"'\"'b' " +
      bbp::PosixShellQuote("$(touch " + execution_marker.string() + ")");
  bbp::OwnedFiroQtLauncher launcher = bbp::OwnedFiroQtLauncher::Create(command);
  const std::filesystem::path launcher_path = launcher.path();
  ScopedFileRemoval remove_launcher_on_failure(launcher_path);

  BOOST_TEST(launcher.active());
  BOOST_TEST(launcher_path.parent_path() == "/tmp");
  BOOST_TEST(launcher_path.filename().string().starts_with("bbp-firo-qt-"));
  BOOST_TEST(launcher_path.extension() == ".sh");
  BOOST_TEST(launcher_path != foreign_path);
  BOOST_TEST(ReadFile(foreign_path) == "foreign launcher\n");
  BOOST_TEST(ReadFile(launcher_path) == "#!/bin/bash\nexec " + command + "\n");
  BOOST_TEST(!std::filesystem::exists(execution_marker));

  struct stat status {};
  BOOST_REQUIRE(lstat(launcher_path.c_str(), &status) == 0);
  BOOST_CHECK(S_ISREG(status.st_mode));
  BOOST_TEST((status.st_mode & 07777) == S_IRWXU);

  BOOST_CHECK(launcher.Cleanup() == bbp::FiroQtLauncherCleanupResult::kRemoved);
  BOOST_TEST(!launcher.active());
  errno = 0;
  BOOST_TEST(lstat(launcher_path.c_str(), &status) == -1);
  BOOST_TEST(errno == ENOENT);
  BOOST_TEST(ReadFile(foreign_path) == "foreign launcher\n");
}

BOOST_AUTO_TEST_CASE(firo_qt_launcher_preserves_replaced_foreign_identity) {
  bbp::OwnedFiroQtLauncher launcher =
      bbp::OwnedFiroQtLauncher::Create("'/opt/firo/firo-qt' '-regtest'");
  const std::filesystem::path launcher_path = launcher.path();
  ScopedFileRemoval remove_replacement(launcher_path);
  BOOST_REQUIRE(std::filesystem::remove(launcher_path));
  WriteNewFile(launcher_path, "replacement owned by somebody else\n");

  BOOST_CHECK(launcher.Cleanup() ==
              bbp::FiroQtLauncherCleanupResult::kOwnershipChanged);
  BOOST_TEST(!launcher.active());
  BOOST_TEST(ReadFile(launcher_path) == "replacement owned by somebody else\n");
}

BOOST_AUTO_TEST_CASE(firo_qt_launcher_destructor_removes_exact_owned_file) {
  std::filesystem::path launcher_path;
  {
    bbp::OwnedFiroQtLauncher launcher =
        bbp::OwnedFiroQtLauncher::Create("'/opt/firo/firo-qt' '-regtest'");
    launcher_path = launcher.path();
    BOOST_REQUIRE(std::filesystem::exists(launcher_path));
  }
  struct stat status {};
  errno = 0;
  BOOST_TEST(lstat(launcher_path.c_str(), &status) == -1);
  BOOST_TEST(errno == ENOENT);
}
