#pragma once

#ifdef BBP_ENABLE_TEST_HOOKS
#include <functional>
#endif

namespace bbp {

class Cgroup;
struct ResourceLimits;

namespace simulator_app_internal {

void VerifyResourceLimits(const Cgroup& cgroup, const ResourceLimits& expected);

void WriteResourceLimits(const Cgroup& cgroup, const ResourceLimits& previous,
                         const ResourceLimits& next);

#ifdef BBP_ENABLE_TEST_HOOKS
void SetResourceLimitsAppliedHookForTest(std::function<void()> hook);
#endif

}  // namespace simulator_app_internal
}  // namespace bbp
