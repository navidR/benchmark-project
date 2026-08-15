#pragma once

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <stdexcept>
#include <stop_token>

namespace bbp {

class ChainDriver;
class RuntimeNodeSnapshot;
struct Options;
struct SendRawTransactionWorkload;
struct SimulationCommandControl;

namespace simulator_app_internal {

class TransactionObservationTracker;

class OneShotRawTransactionRejected final : public std::runtime_error {
 public:
  OneShotRawTransactionRejected()
      : std::runtime_error(
            "raw transaction broadcast was deterministically rejected") {}
};

void ApplySendRawTransactionWorkload(
    const Options& options, const std::filesystem::path& events_path,
    const ChainDriver& driver, std::timed_mutex& block_generation_mutex,
    const RuntimeNodeSnapshot& nodes,
    TransactionObservationTracker& transaction_tracker,
    const SendRawTransactionWorkload& workload, std::uint32_t workload_index,
    std::uint32_t workload_count, std::stop_token stop_token,
    SimulationCommandControl* cancellation_commit_control = nullptr);

}  // namespace simulator_app_internal
}  // namespace bbp
