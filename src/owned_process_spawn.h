#pragma once

#include <sys/types.h>

#include <memory>
#include <type_traits>

namespace bbp {

// Fork from a process-lifetime thread so caller-thread exit cannot trigger the
// child's parent-death signal. Returns only in the parent, with fork-style
// PID/errno results. The child callback must exec or _exit.
pid_t ForkOwnedProcess(void (*child)(void*), void* context);

#ifdef BBP_ENABLE_TEST_HOOKS
void SetOwnedForkBeforeParentDeathHookForTest(void (*hook)());
#endif

template <typename Child>
pid_t ForkOwnedProcess(Child&& child) {
  return ForkOwnedProcess(
      [](void* context) {
        (*static_cast<std::remove_reference_t<Child>*>(context))();
      },
      std::addressof(child));
}

}  // namespace bbp
