#include "simulator_node_report_export.h"

#include <filesystem>
#include <stdexcept>
#include <system_error>

#include "bbp/run_report.h"
#include "bbp/simulation_command.h"
#include "bbp/util.h"
#include "simulator_scheduled_command_event_details.h"

namespace bbp::simulator_app_internal {

void ExportNodeReport(const std::filesystem::path& run_root,
                      const SimulationCommand& command) {
  const std::filesystem::path relative = NodeReportRelativePath(command);
  const std::filesystem::path output = run_root / relative;
  EnsureDirectory(output.parent_path());
  std::filesystem::path temporary = output;
  temporary += ".tmp";
  std::error_code ec;
  std::filesystem::remove(temporary, ec);
  ec.clear();
  try {
    WriteText(temporary,
              BuildNodeReportJson(run_root, command.node_id, command.sequence) +
                  "\n");
    std::filesystem::rename(temporary, output, ec);
    if (ec) {
      throw std::runtime_error("rename node report failed: " + ec.message());
    }
  } catch (...) {
    std::filesystem::remove(temporary, ec);
    throw;
  }
}

}  // namespace bbp::simulator_app_internal
