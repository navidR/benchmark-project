#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <unistd.h>

#include <boost/test/unit_test.hpp>
#include <cerrno>
#include <filesystem>
#include <stdexcept>
#include <string>
#include <vector>

#include "bbp/application_instance_lock.h"

namespace bbp {
namespace {

class InstanceLockTestDirectory {
 public:
  InstanceLockTestDirectory() {
    std::string pattern = (std::filesystem::temp_directory_path() /
                           "bbp-instance-lock-test-XXXXXX")
                              .string();
    std::vector<char> writable(pattern.begin(), pattern.end());
    writable.push_back('\0');
    char* created = mkdtemp(writable.data());
    if (created == nullptr) {
      throw std::runtime_error("mkdtemp failed for instance-lock test");
    }
    path_ = created;
  }

  ~InstanceLockTestDirectory() { std::filesystem::remove_all(path_); }

  const std::filesystem::path& path() const { return path_; }

 private:
  std::filesystem::path path_;
};

int ChildAcquisitionResult(const std::filesystem::path& state_directory) {
  const pid_t child = fork();
  if (child < 0) {
    throw std::runtime_error("fork failed for instance-lock test");
  }
  if (child == 0) {
    try {
      ApplicationInstanceLock lock(state_directory);
      _exit(0);
    } catch (const std::exception& error) {
      const std::string message = error.what();
      _exit(message.find("already running") != std::string::npos ? 2 : 3);
    } catch (...) {
      _exit(4);
    }
  }

  int status = 0;
  pid_t waited = -1;
  do {
    waited = waitpid(child, &status, 0);
  } while (waited < 0 && errno == EINTR);
  if (waited != child || !WIFEXITED(status)) {
    throw std::runtime_error("instance-lock child did not exit normally");
  }
  return WEXITSTATUS(status);
}

mode_t Permissions(const std::filesystem::path& path) {
  struct stat status{};
  if (lstat(path.c_str(), &status) != 0) {
    throw std::runtime_error("inspect instance-lock test path failed");
  }
  return status.st_mode & 0777;
}

}  // namespace

BOOST_AUTO_TEST_CASE(
    application_instance_lock_rejects_a_second_process_and_releases_on_exit) {
  InstanceLockTestDirectory temporary;
  const std::filesystem::path state_directory = temporary.path() / ".bbp";
  {
    ApplicationInstanceLock lock(state_directory);
    BOOST_TEST(lock.state_directory() == state_directory);
    BOOST_TEST(ChildAcquisitionResult(state_directory) == 2);
  }
  BOOST_TEST(ChildAcquisitionResult(state_directory) == 0);
}

BOOST_AUTO_TEST_CASE(
    application_instance_lock_secures_persistent_state_and_lock_files) {
  InstanceLockTestDirectory temporary;
  const std::filesystem::path state_directory = temporary.path() / ".bbp";
  std::filesystem::create_directory(state_directory);
  BOOST_REQUIRE(chmod(state_directory.c_str(), 0755) == 0);
  const std::filesystem::path lock_path = state_directory / "bbp.lock";
  const int descriptor =
      open(lock_path.c_str(), O_CREAT | O_WRONLY | O_CLOEXEC, 0644);
  BOOST_REQUIRE(descriptor >= 0);
  BOOST_REQUIRE(close(descriptor) == 0);
  BOOST_REQUIRE(chmod(lock_path.c_str(), 0644) == 0);

  {
    ApplicationInstanceLock lock(state_directory);
    BOOST_TEST(Permissions(state_directory) == 0700);
    BOOST_TEST(Permissions(lock_path) == 0600);
  }
  BOOST_TEST(std::filesystem::is_regular_file(lock_path));
}

BOOST_AUTO_TEST_CASE(
    application_instance_lock_rejects_symlink_and_nonregular_lock_paths) {
  InstanceLockTestDirectory temporary;
  const std::filesystem::path target = temporary.path() / "target";
  const std::filesystem::path linked_state = temporary.path() / "linked";
  std::filesystem::create_directory(target);
  std::filesystem::create_directory_symlink(target, linked_state);
  BOOST_CHECK_THROW(static_cast<void>(ApplicationInstanceLock{linked_state}),
                    std::runtime_error);

  const std::filesystem::path state_directory = temporary.path() / ".bbp";
  std::filesystem::create_directory(state_directory);
  std::filesystem::create_directory(state_directory / "bbp.lock");
  BOOST_CHECK_THROW(static_cast<void>(ApplicationInstanceLock{state_directory}),
                    std::runtime_error);
}

}  // namespace bbp
