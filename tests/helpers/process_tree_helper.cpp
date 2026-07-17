#include <fcntl.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>

namespace {

bool WriteExact(int fd, const char* data, std::size_t size) {
  while (size != 0U) {
    const ssize_t written = write(fd, data, size);
    if (written < 0) {
      if (errno == EINTR) {
        continue;
      }
      return false;
    }
    if (written == 0) {
      return false;
    }
    data += written;
    size -= static_cast<std::size_t>(written);
  }
  return true;
}

bool WriteFile(const char* path, const std::string& contents) {
  const int fd =
      open(path, O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC | O_NOFOLLOW, 0600);
  if (fd < 0) {
    return false;
  }
  const bool written = WriteExact(fd, contents.data(), contents.size());
  const bool closed = close(fd) == 0;
  return written && closed;
}

int WaitForTermination(const sigset_t& signals, const char* marker) {
  int signal = 0;
  if (sigwait(&signals, &signal) != 0 || signal != SIGTERM) {
    return 1;
  }
  return WriteFile(marker, "SIGTERM") ? 0 : 1;
}

}  // namespace

int main(int argc, char** argv) {
  const bool ignore_term =
      argc == 5 && std::strcmp(argv[4], "--ignore-term") == 0;
  if (argc != 4 && !ignore_term) {
    return 2;
  }
  if (ignore_term) {
    if (signal(SIGTERM, SIG_IGN) == SIG_ERR) {
      return 3;
    }
  }
  sigset_t signals;
  sigemptyset(&signals);
  sigaddset(&signals, SIGTERM);
  if (!ignore_term && sigprocmask(SIG_BLOCK, &signals, nullptr) != 0) {
    return 3;
  }

  const pid_t descendant = fork();
  if (descendant < 0) {
    return 4;
  }
  if (descendant == 0) {
    if (ignore_term) {
      while (true) {
        pause();
      }
    }
    _exit(WaitForTermination(signals, argv[3]));
  }
  if (!WriteFile(argv[1], std::to_string(descendant))) {
    kill(descendant, SIGKILL);
    waitpid(descendant, nullptr, 0);
    return 5;
  }

  if (ignore_term) {
    while (true) {
      pause();
    }
  }

  const int result = WaitForTermination(signals, argv[2]);
  int status = 0;
  while (waitpid(descendant, &status, 0) < 0) {
    if (errno != EINTR) {
      return 6;
    }
  }
  if (result != 0 || !WIFEXITED(status) || WEXITSTATUS(status) != 0) {
    return 7;
  }
  return 0;
}
