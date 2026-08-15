#pragma once

namespace bbp {

class Cgroup;
struct ResourceLimits;

namespace simulator_app_internal {

void VerifyResourceLimits(const Cgroup& cgroup, const ResourceLimits& expected);

void WriteResourceLimits(const Cgroup& cgroup, const ResourceLimits& previous,
                         const ResourceLimits& next);

}  // namespace simulator_app_internal
}  // namespace bbp
