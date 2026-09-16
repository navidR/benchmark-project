#include "owned_process_spawn.h"

#include <fcntl.h>
#include <pthread.h>
#include <signal.h>
#include <sys/prctl.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cerrno>
#include <condition_variable>
#include <mutex>
#include <new>
#include <system_error>
#include <thread>

namespace bbp {
namespace {

#ifdef BBP_ENABLE_TEST_HOOKS
std::atomic<void (*)()> before_parent_death_hook = nullptr;
#endif

struct ForkRequest {
  void (*child)(void*);
  void* context;
  pid_t pid = -1;
  int error = 0;
  bool completed = false;
};

void ForkAndInstallParentDeath(ForkRequest* request) {
  int status_pipe[2];
  if (pipe2(status_pipe, O_CLOEXEC) != 0) {
    request->error = errno;
    return;
  }
  const pid_t parent = getpid();
  const pid_t pid = fork();
  if (pid == 0) {
    close(status_pipe[0]);
#ifdef BBP_ENABLE_TEST_HOOKS
    if (const auto hook = before_parent_death_hook.load()) {
      hook();
    }
#endif
    int status = 0;
    if (prctl(PR_SET_PDEATHSIG, SIGKILL) != 0) {
      status = errno;
    } else if (getppid() != parent) {
      _exit(127);
    }
    ssize_t written;
    do {
      written = write(status_pipe[1], &status, sizeof(status));
    } while (written < 0 && errno == EINTR);
    close(status_pipe[1]);
    if (status != 0 || written != static_cast<ssize_t>(sizeof(status))) {
      _exit(127);
    }
    request->child(request->context);
    _exit(127);
  }
  const int fork_error = errno;
  close(status_pipe[1]);
  if (pid < 0) {
    close(status_pipe[0]);
    request->error = fork_error;
    return;
  }
  int status = 0;
  ssize_t received;
  do {
    received = read(status_pipe[0], &status, sizeof(status));
  } while (received < 0 && errno == EINTR);
  const int read_error = errno;
  close(status_pipe[0]);
  if (received != static_cast<ssize_t>(sizeof(status)) || status != 0) {
    request->error = received < 0 ? read_error : status != 0 ? status : ECHILD;
    static_cast<void>(kill(pid, SIGKILL));
    while (waitpid(pid, nullptr, 0) < 0 && errno == EINTR) {
    }
    return;
  }
  request->pid = pid;
}

class ForkOwner {
 public:
  ForkOwner() : worker_([this] { Run(); }) {}

  pid_t Spawn(void (*child)(void*), void* context) {
    ForkRequest request{.child = child, .context = context};
    std::unique_lock lock(mutex_);
    changed_.wait(lock, [this] { return pending_ == nullptr; });
    pending_ = &request;
    changed_.notify_all();
    changed_.wait(lock, [&request] { return request.completed; });
    errno = request.error;
    return request.pid;
  }

 private:
  void Run() {
    std::unique_lock lock(mutex_);
    for (;;) {
      changed_.wait(lock, [this] { return pending_ != nullptr; });
      ForkAndInstallParentDeath(pending_);
      pending_->completed = true;
      pending_ = nullptr;
      changed_.notify_all();
    }
  }

  std::mutex mutex_;
  std::condition_variable changed_;
  ForkRequest* pending_ = nullptr;
  std::thread worker_;
};

pthread_mutex_t owner_mutex = PTHREAD_MUTEX_INITIALIZER;
ForkOwner* owner = nullptr;
bool fork_handlers_installed = false;

void LockOwner() { static_cast<void>(pthread_mutex_lock(&owner_mutex)); }
void UnlockOwner() { static_cast<void>(pthread_mutex_unlock(&owner_mutex)); }
void ResetOwnerInChild() {
  // The old owner's thread does not exist in a fork child. Its copied state
  // must never be locked or destroyed. A child that launches again gets its
  // own owner; normal exec/_exit releases the copied address space.
  owner = nullptr;
  UnlockOwner();
}

ForkOwner* ProcessForkOwner() {
  LockOwner();
  try {
    if (!fork_handlers_installed) {
      const int error =
          pthread_atfork(LockOwner, UnlockOwner, ResetOwnerInChild);
      if (error != 0) {
        throw std::system_error(error, std::generic_category(),
                                "register owned fork handlers");
      }
      fork_handlers_installed = true;
    }
    if (owner == nullptr) {
      // Deliberately retain the owner until process exit. Destroying its
      // thread earlier would signal otherwise healthy managed children.
      owner = new ForkOwner;
    }
    ForkOwner* result = owner;
    UnlockOwner();
    return result;
  } catch (...) {
    UnlockOwner();
    throw;
  }
}

}  // namespace

#ifdef BBP_ENABLE_TEST_HOOKS
void SetOwnedForkBeforeParentDeathHookForTest(void (*hook)()) {
  before_parent_death_hook.store(hook);
}
#endif

pid_t ForkOwnedProcess(void (*child)(void*), void* context) {
  try {
    return ProcessForkOwner()->Spawn(child, context);
  } catch (const std::system_error& error) {
    errno = error.code().value();
  } catch (const std::bad_alloc&) {
    errno = ENOMEM;
  }
  return -1;
}

}  // namespace bbp
