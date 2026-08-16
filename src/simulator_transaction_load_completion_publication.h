#pragma once

#include <filesystem>
#include <vector>

#include "simulator_wallet_transaction_workload_execution.h"

namespace bbp {

struct Options;

namespace simulator_app_internal {

void WriteTransactionLoadCompletions(
    const Options& options, const std::filesystem::path& events_path,
    const std::vector<PendingTransactionLoadCompletion>& completions);

}  // namespace simulator_app_internal
}  // namespace bbp
