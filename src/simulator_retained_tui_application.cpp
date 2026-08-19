#include "simulator_retained_tui_application.h"

#include <boost/json/object.hpp>
#include <cstdint>
#include <exception>
#include <filesystem>
#include <stdexcept>
#include <string>

#include "bbp/chain_kind.h"
#include "bbp/logging.h"
#include "bbp/mcp_endpoint.h"
#include "bbp/mcp_live_application.h"
#include "bbp/run_ownership.h"
#include "bbp/run_report.h"
#include "bbp/signal_stop_monitor.h"
#include "bbp/tui.h"
#include "simulator_json_field_decoding.h"
#include "simulator_scenario_identifier.h"

namespace bbp::simulator_app_internal {
namespace {

struct RetainedRunContext {
  std::string run_id;
  McpLiveApplication::RetainedRun metadata;
};

RetainedRunContext LoadRetainedRunContext(
    const std::filesystem::path& run_root) {
  const boost::json::object report = BuildRunReport(run_root);
  const std::string run_id = JsonStringField(report, "run_id");
  RequireSafeScenarioIdentifier(run_id, "retained run id");
  const std::string chain = JsonStringField(report, "chain");
  static_cast<void>(ParseChainKind(chain));
  const std::string state = JsonStringField(report, "status");
  if (state != "finished" && state != "failed" && state != "cancelled" &&
      state != "incomplete") {
    throw std::runtime_error("invalid retained run status: " + state);
  }
  bool has_owned_artifacts = false;
  try {
    static_cast<void>(LoadRunOwnership(run_id, run_root));
    has_owned_artifacts = true;
  } catch (const std::exception&) {
    // Reports from older or externally copied runs remain readable, but their
    // files are not exposed without a valid ownership marker.
  }
  return RetainedRunContext{.run_id = run_id,
                            .metadata = McpLiveApplication::RetainedRun{
                                .chain = chain,
                                .node_count = JsonUint32Field(report, "nodes"),
                                .state = state,
                                .has_owned_artifacts = has_owned_artifacts}};
}

}  // namespace

int RunRetainedTuiWithMcp(const std::filesystem::path& run_root,
                          const std::filesystem::path& state_directory,
                          bool once, std::uint32_t refresh_ms) {
  const RetainedRunContext retained = LoadRetainedRunContext(run_root);
  SignalStopMonitor signal_monitor;
  McpLiveApplication mcp_application(
      McpLiveApplication::Config{.run_id = retained.run_id,
                                 .run_root = run_root,
                                 .retained_run = retained.metadata,
                                 .options = {},
                                 .command_queue = {},
                                 .node_inventory_snapshot = {},
                                 .publication_mutex = {},
                                 .request_run_stop = {},
                                 .run_started = {},
                                 .run_stopping = {},
                                 .run_stopped = {}});
  McpEndpoint mcp_endpoint(
      McpEndpointConfig{
          .state_directory = state_directory,
          .run_id = retained.run_id,
          .server = {},
          .dispatcher = {},
          .allowed_operations = mcp_application.SupportedOperations(),
          .allowed_information_families =
              mcp_application.SupportedInformationFamilies(),
          .read_only = mcp_application.read_only()},
      mcp_application.OperationFactory(), mcp_application.ResourceReader());

  SetConsoleLoggingEnabled(false);
  std::exception_ptr application_failure;
  int result = 1;
  try {
    mcp_endpoint.Start();
    const McpEndpointPublication publication = mcp_endpoint.publication();
    const TuiMcpConnectionInfo mcp_connection{
        .endpoint = publication.endpoint,
        .token_file = publication.token_file,
        .client_config_file = publication.client_config_file,
    };
    result = RunTuiReport(run_root, once, refresh_ms, mcp_connection, nullptr,
                          signal_monitor.GetToken());
  } catch (...) {
    application_failure = std::current_exception();
  }

  std::exception_ptr cleanup_failure;
  const auto capture_cleanup_failure = [&](auto&& action) {
    try {
      action();
    } catch (...) {
      if (!cleanup_failure) {
        cleanup_failure = std::current_exception();
      }
    }
  };
  bool endpoint_drained = false;
  capture_cleanup_failure([&] {
    mcp_endpoint.StopAdmissionAndDrain();
    endpoint_drained = true;
  });
  if (endpoint_drained) {
    capture_cleanup_failure([&] { mcp_application.Shutdown(); });
    capture_cleanup_failure([&] { mcp_endpoint.Stop(); });
  }
  SetConsoleLoggingEnabled(true);

  if (application_failure) {
    std::rethrow_exception(application_failure);
  }
  if (cleanup_failure) {
    std::rethrow_exception(cleanup_failure);
  }
  if (signal_monitor.ReceivedSignal() != 0) {
    BBP_LOG(info) << "graceful shutdown completed after signal "
                  << signal_monitor.ReceivedSignal();
  }
  return result;
}

}  // namespace bbp::simulator_app_internal
