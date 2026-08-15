#include "simulator_operator_connection_publication.h"

#include <boost/json/array.hpp>
#include <boost/json/object.hpp>
#include <boost/json/serialize.hpp>
#include <optional>
#include <string>
#include <utility>

#include "bbp/chain_network.h"
#include "bbp/drivers/chain_driver.h"
#include "bbp/logging.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/simulation_event_kind.h"
#include "bbp/simulator/options.h"
#include "simulator_event_writing.h"
#include "simulator_node_process_state.h"

namespace bbp::simulator_app_internal {

void PublishOperatorConnectionCommand(const Options& options,
                                      const std::filesystem::path& run_root,
                                      const std::filesystem::path& events_path,
                                      const ChainDriver& driver,
                                      const RuntimeNodeSnapshot& nodes,
                                      bool* resolved) {
  if (*resolved) {
    return;
  }
  for (const NodeRuntime& node : nodes) {
    {
      auto process_guard = LockNodeProcessState(node);
      if (!node.AllowsChainMetrics() || !node.process.running()) {
        continue;
      }
    }
    const std::optional<OperatorConnectionCommand> connection =
        driver.BuildOperatorConnectionCommand(node.config, run_root);
    *resolved = true;
    if (!connection) {
      return;
    }

    boost::json::array arguments;
    arguments.reserve(connection->arguments.size());
    for (const std::string& argument : connection->arguments) {
      arguments.emplace_back(argument);
    }
    boost::json::array argv;
    argv.reserve(connection->arguments.size() + 1U);
    argv.emplace_back(connection->executable.string());
    for (const std::string& argument : connection->arguments) {
      argv.emplace_back(argument);
    }
    boost::json::object detail;
    detail["kind"] = "manual_firo_gui";
    detail["manual_launch"] = true;
    detail["discovery_disabled"] = true;
    detail["wallet_enabled"] = true;
    detail["network"] = ChainNetworkName(node.config.network);
    detail["executable"] = connection->executable.string();
    detail["arguments"] = std::move(arguments);
    detail["argv"] = std::move(argv);
    detail["command"] = connection->ShellCommand();
    detail["data_dir"] = connection->data_dir.string();
    detail["peer_address"] = connection->peer_address;
    detail["peer_port"] = connection->peer_port;
    detail["peer_endpoint"] =
        connection->peer_address + ":" + std::to_string(connection->peer_port);
    WriteEvent(events_path, options.run_id, node.config.id,
               SimulationEventKind::kOperatorConnectionCommand,
               boost::json::serialize(detail));
    BBP_LOG(info) << "manual Firo GUI command: " << connection->ShellCommand();
    return;
  }
}

}  // namespace bbp::simulator_app_internal
