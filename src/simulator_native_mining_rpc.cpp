#include "simulator_native_mining_rpc.h"

#include <exception>
#include <optional>
#include <stdexcept>
#include <thread>

#include "bbp/drivers/chain_driver.h"
#include "bbp/run_process_state.h"
#include "bbp/simulation_cancelled.h"
#include "bbp/simulator/node_runtime.h"
#include "simulator_cancellable_waiting.h"
#include "simulator_node_process_state.h"

namespace bbp::simulator_app_internal {

bool StartNativeMiningForCurrentProcess(
    const ChainDriver& driver, NodeRuntime& node,
    RunProcessState& run_process_state, std::string_view reward_address,
    std::stop_token stop_token, std::string_view action,
    std::optional<std::chrono::steady_clock::time_point> lock_deadline,
    bool* rpc_attempted) {
  std::optional<RunProcessState::NativeMiningRpcGuard> mining_rpc_guard;
  if (lock_deadline) {
    mining_rpc_guard = run_process_state.TryLockNativeMiningRpcUntil(
        *lock_deadline, stop_token);
    if (!mining_rpc_guard) {
      ThrowIfStopRequested(stop_token);
      throw std::runtime_error("native mining RPC lock deadline expired: " +
                               node.config.id);
    }
  } else {
    mining_rpc_guard.emplace(run_process_state.LockNativeMiningRpc());
  }
  NodeProcessGeneration generation;
  {
    auto process_guard = run_process_state.Lock();
    generation = RunningNodeProcessGeneration(node, process_guard, action);
  }

  if (rpc_attempted != nullptr) {
    *rpc_attempted = true;
  }
  driver.StartMining(node.config, std::string(reward_address), stop_token);

  bool current_process_running = false;
  {
    auto process_guard = run_process_state.Lock();
    if (IsCurrentRunningNodeProcess(node, process_guard, generation)) {
      run_process_state.AddActiveNativeMiner(process_guard, node.config.id);
      return true;
    }
    current_process_running =
        node.AllowsChainMetrics() && node.process.running();
  }

  // The successful RPC no longer belongs to the process generation that
  // authorized it. If a replacement is live, undo any request that may have
  // reached that replacement before allowing its own serialized mining RPC.
  if (current_process_running) {
    driver.StopMining(node.config, stop_token);
  }
  return false;
}

void StopNativeMining(const ChainDriver& driver, NodeRuntime& node,
                      RunProcessState& run_process_state,
                      std::stop_token stop_token) {
  auto mining_rpc_guard = run_process_state.LockNativeMiningRpc();
  driver.StopMining(node.config, stop_token);
  auto process_guard = run_process_state.Lock();
  run_process_state.RemoveActiveNativeMiner(process_guard, node.config.id);
}

bool StopNativeMiningBeforeDeadline(
    const ChainDriver& driver, const NodeRuntime& node,
    std::chrono::steady_clock::time_point deadline, std::string* error) {
  if (std::chrono::steady_clock::now() >= deadline) {
    *error = "native mining compensation deadline expired";
    return false;
  }
  std::stop_source operation_stop_source;
  std::jthread deadline_timer(
      [deadline, &operation_stop_source](std::stop_token timer_stop_token) {
        try {
          WaitUntil(deadline, timer_stop_token);
        } catch (const SimulationCancelled&) {
          return;
        }
        operation_stop_source.request_stop();
      });
  try {
    driver.StopMining(node.config, operation_stop_source.get_token());
  } catch (const std::exception& failure) {
    deadline_timer.request_stop();
    deadline_timer.join();
    *error = failure.what();
    return false;
  } catch (...) {
    deadline_timer.request_stop();
    deadline_timer.join();
    *error = "unknown native mining compensation failure";
    return false;
  }
  deadline_timer.request_stop();
  deadline_timer.join();
  return true;
}

}  // namespace bbp::simulator_app_internal
