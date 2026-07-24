#pragma once

namespace bbp {

class SimulatorApp {
 public:
  int Run(int argc, char** argv);
};

#ifdef BBP_ENABLE_TEST_HOOKS
bool RuntimeNodeSupportDestructionAllowedForTest(
    bool daemon_absence_verified, bool exact_cgroup_acquired,
    bool exact_cgroup_empty);
#endif

}  // namespace bbp
