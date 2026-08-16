#include "simulator_transaction_load_completion_publication.h"

#include <stdexcept>

#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "bbp/simulator/transaction_load.h"
#include "simulator_event_writing.h"
#include "simulator_workload_event_details.h"

namespace bbp::simulator_app_internal {

void WriteTransactionLoadCompletions(
    const Options& options, const std::filesystem::path& events_path,
    const std::vector<PendingTransactionLoadCompletion>& completions) {
  for (const PendingTransactionLoadCompletion& completion : completions) {
    if (!completion.accounting) {
      throw std::runtime_error(
          "pending transaction load completion has no accounting");
    }
    const TransactionLoadSnapshot snapshot =
        completion.accounting->Snapshot(completion.elapsed);
    WriteEvent(events_path, options.run_id, "sim",
               SimulationEventKind::kTransactionLoadCompleted,
               TransactionLoadCompletedDetail(
                   completion.workload_index, completion.workload_count,
                   completion.workload, completion.attempt_limit,
                   completion.queue_maximum_size, snapshot));
  }
}

}  // namespace bbp::simulator_app_internal
