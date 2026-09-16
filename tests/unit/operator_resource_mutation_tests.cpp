#include <unistd.h>

#include <atomic>
#include <boost/json/parse.hpp>
#include <boost/json/serialize.hpp>
#include <boost/test/unit_test.hpp>
#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <sstream>
#include <stop_token>
#include <string>
#include <thread>
#include <vector>

#include "../../src/simulator_cancellable_waiting.h"
#include "../../src/simulator_live_command_execution.h"
#include "../../src/simulator_live_instrumentation_controller.h"
#include "../../src/simulator_resource_limit_application.h"
#include "../../src/simulator_runtime_node_addition.h"
#include "../../src/simulator_runtime_node_removal.h"
#include "../../src/simulator_runtime_node_replacement.h"
#include "../../src/simulator_transaction_observation_tracking.h"
#include "bbp/drivers/chain_command_executor.h"
#include "bbp/drivers/chain_driver_registry.h"
#include "bbp/mcp_dispatcher.h"
#include "bbp/mcp_live_application.h"
#include "bbp/peer_connectivity_controller.h"
#include "bbp/probabilistic_block_scheduler.h"
#include "bbp/runtime_node_inventory.h"
#include "bbp/runtime_peer_topology.h"
#include "bbp/runtime_wallet_registry.h"
#include "bbp/scenario_service.h"
#include "bbp/simulation_command_processor.h"
#include "bbp/simulator/options.h"
#include "bbp/util.h"

namespace {

using namespace bbp;
using namespace bbp::simulator_app_internal;
using namespace std::chrono_literals;

class ResourceRun {
 public:
  ResourceRun() {
    static std::atomic_uint sequence{0U};
    id = "res-test-" + std::to_string(getpid()) + "-" +
         std::to_string(sequence.fetch_add(1U));
    path = std::filesystem::temp_directory_path() / id;
    std::filesystem::create_directory(path);
  }
  bool Prepare() {
    RequireSafeRunId(id);
    try {
      Cgroup::PrepareRun(id);
      prepared = true;
    } catch (const std::exception& error) {
      BOOST_TEST_MESSAGE(
          "skipping privileged resource mutation test: " << error.what());
    }
    return prepared;
  }
  ~ResourceRun() {
    try {
      if (prepared) {
        Cgroup::RemoveRun(id);
      }
      std::filesystem::remove_all(path);
    } catch (const std::exception& error) {
      BOOST_ERROR("resource fixture cleanup failed: " << error.what());
    }
  }
  std::string id;
  std::filesystem::path path;
  bool prepared = false;
};

template <typename Predicate>
void Await(Predicate predicate) {
  const auto deadline = std::chrono::steady_clock::now() + 5s;
  while (!predicate()) {
    if (std::chrono::steady_clock::now() >= deadline) {
      throw std::runtime_error("resource command test timed out");
    }
    std::this_thread::sleep_for(1ms);
  }
}

// Real command routing and cgroup writes; unused chain operations stay dormant.
class ResourceCommands {
 public:
  ResourceCommands() {
    if (!run.Prepare()) {
      return;
    }
    options =
        std::make_shared<Options>(ParseAndValidateScenario(boost::json::object{
            {"chain", "firo"},
            {"chain_daemon", "/bin/true"},
            {"run_id", run.id},
            {"nodes", 1U},
            {"block_production", boost::json::object{{"enabled", false}}}}));
    initial = ResourceLimits{.memory_high_bytes = 64U * 1024U * 1024U,
                             .memory_max_bytes = 128U * 1024U * 1024U,
                             .cpu_quota_us = 50000U,
                             .cpu_period_us = 100000U,
                             .cpu_weight = 100U,
                             .io_weight = 100U,
                             .io_limits = {},
                             .pids_max = 128U};
    desired = initial;
    desired.memory_high_bytes /= 2U;
    desired.cpu_quota_us = 60000U;
    options->resource_profiles.emplace("changed", desired);
    std::vector<NodeRuntime> nodes(1U);
    NodeRuntime& node = nodes.front();
    node.config.id = "firo-1";
    node.run_process_state = &process_state;
    node.resource_profile = "original";
    node.resources = initial;
    node.cgroup = Cgroup::CreateShared(run.id, node.config.id);
    node.cgroup->SetMemoryMax(initial.memory_max_bytes);
    node.cgroup->SetMemoryHigh(initial.memory_high_bytes);
    node.cgroup->SetCpuMax(initial.cpu_quota_us, initial.cpu_period_us);
    node.cgroup->SetPidsMax(initial.pids_max);
    VerifyResourceLimits(*node.cgroup, initial);
    inventory.Initialize(nodes);
    application =
        std::make_unique<McpLiveApplication>(McpLiveApplication::Config{
            .run_id = run.id,
            .run_root = run.path,
            .retained_run = std::nullopt,
            .options = options,
            .command_queue = queue,
            .node_inventory_snapshot =
                [] {
                  return McpLiveNodeInventorySnapshot{.generation = 1U,
                                                      .node_ids = {"firo-1"},
                                                      .node_capacity = 1U};
                },
            .publication_mutex = {},
            .request_run_stop = [&] { run_stop_requested = true; },
            .run_started = {},
            .run_stopping = {},
            .run_stopped = {}});
    application->MarkRunStarted();
    const LiveCommandExecutionContext context{
        .options = *options,
        .run_root = run.path,
        .events_path = events_path,
        .chain_spec = DefaultChainDriverSpec(),
        .driver = *driver,
        .mcp_application = *application,
        .node_inventory = inventory,
        .runtime_wallet_registry = wallets,
        .block_scheduler = scheduler,
        .peer_connectivity_controller = peers,
        .chain_command_executor = chain_executor,
        .configured_miner_node_ids_mutex = miner_mutex,
        .miner_node_ids = miner_ids,
        .node_mutation_mutex = mutation_mutex,
        .block_generation_mutex = generation_mutex,
        .runtime_topology_mutex = topology_mutex,
        .node_network_state_mutex = network_mutex,
        .node_resource_state_mutex = resource_mutex,
        .runtime_topology = topology,
        .live_topology_config = topology_config,
        .run_process_state = process_state,
        .lifecycle_epoch = epoch,
        .command_rpc_stop_source = command_stop,
        .wallets_initialized = wallets_initialized,
        .transaction_tracker = transactions,
        .wallet_workloads = wallet_workloads,
        .block_generation_workloads = block_workloads,
        .wait_until_height_workloads = height_workloads,
        .wait_for_peers_workloads = peer_workloads,
        .live_instrumentation = instrumentation,
        .runtime_node_addition_dependencies = addition,
        .runtime_node_removal_dependencies = removal,
        .runtime_node_replacement_dependencies = replacement,
        .command_role_service = roles,
        .is_configured_miner = [](std::string_view) { return false; },
        .request_simulation_stop =
            [&] {
              run_stop_requested = true;
              return true;
            },
        .record_scheduled_command_outcome = [](const auto&, auto) {},
        .acquire_node_mutation_lock =
            [](std::timed_mutex& mutex, std::stop_token token) {
              ThrowIfStopRequested(token);
              return std::unique_lock<std::timed_mutex>(mutex);
            },
        .acquire_runtime_publication_lock = nullptr,
        .find_node_runtime_by_id = [](const RuntimeNodeSnapshot& snapshot,
                                      const std::string&) -> NodeRuntime& {
          return snapshot.front();
        },
        .exception_message = nullptr,
        .start_node = nullptr};
    processor = MakeLiveSimulationCommandProcessor(*queue, context);
    dispatcher = std::make_unique<McpDispatcher>(
        McpDispatcherConfig{}, application->OperationFactory(),
        application->ResourceReader());
    dispatcher->SessionHandler()("resource-session", true, {});
    processor->Start();
  }

  ~ResourceCommands() {
    if (!run.prepared) {
      return;
    }
    processor->Stop();
    dispatcher->Shutdown();
    application->Shutdown();
    SetResourceLimitsAppliedHookForTest({});
  }

  boost::json::object Invoke(std::string_view tool, boost::json::object args) {
    return dispatcher->ToolHandler()(tool, args, "resource-session", {})
        .as_object();
  }

  boost::json::object Submit(bool profile) {
    boost::json::object command{
        {"kind", profile ? "set_resource_profile" : "set_resource_limits"},
        {"node", "firo-1"}};
    if (profile) {
      command["profile"] = "changed";
    } else {
      command["resource_limits"] =
          boost::json::object{{"memory_high_bytes", desired.memory_high_bytes},
                              {"cpu_quota_us", *desired.cpu_quota_us}};
    }
    return Invoke("simulation.command",
                  {{"run_id", run.id}, {"command", std::move(command)}});
  }

  boost::json::object Inspect(const boost::json::object& submitted) {
    return Invoke("operation.get",
                  {{"operation_id", submitted.at("operation_id")}});
  }

  void Cancel(const boost::json::object& submitted) {
    Invoke("operation.cancel",
           {{"operation_id", submitted.at("operation_id")}});
  }

  boost::json::object Terminal(const boost::json::object& submitted) {
    boost::json::object terminal;
    Await([&] {
      terminal = Inspect(submitted);
      const auto& state = terminal.at("state").as_string();
      return state == "succeeded" || state == "failed" || state == "cancelled";
    });
    return terminal;
  }

  NodeRuntime& Node() { return inventory.Snapshot().front(); }

  void CheckState(const ResourceLimits& expected, std::string_view profile) {
    VerifyResourceLimits(*Node().cgroup, expected);
    BOOST_TEST(Node().resources.memory_high_bytes ==
               expected.memory_high_bytes);
    BOOST_CHECK(Node().resources.cpu_quota_us == expected.cpu_quota_us);
    BOOST_TEST(Node().resource_profile == profile);
  }

  std::vector<std::string> EventKinds() const {
    std::vector<std::string> kinds;
    if (!std::filesystem::exists(events_path)) {
      return kinds;
    }
    std::istringstream lines(ReadText(events_path));
    std::string line;
    while (std::getline(lines, line)) {
      kinds.emplace_back(
          boost::json::parse(line).as_object().at("event").as_string());
    }
    return kinds;
  }

  ResourceRun run;
  std::filesystem::path events_path = run.path / "events.jsonl";
  ResourceLimits initial;
  ResourceLimits desired;
  std::shared_ptr<Options> options;
  std::mutex resource_mutex;
  std::stop_source command_stop;
  std::atomic_bool run_stop_requested{false};

 private:
  RuntimeNodeInventory inventory{1U};
  RuntimeWalletRegistry wallets;
  RunProcessState process_state;
  std::unique_ptr<ChainDriver> driver = CreateDefaultChainDriver();
  std::shared_ptr<SimulationCommandQueue> queue =
      std::make_shared<SimulationCommandQueue>();
  std::unique_ptr<ProbabilisticBlockScheduler> scheduler;
  std::unique_ptr<PeerConnectivityController> peers;
  std::unique_ptr<ChainCommandExecutor> chain_executor;
  std::mutex miner_mutex, topology_mutex, network_mutex;
  std::vector<std::string> miner_ids;
  std::timed_mutex mutation_mutex, generation_mutex;
  std::unique_ptr<RuntimePeerTopology> topology;
  PeerTopologyConfig topology_config;
  std::chrono::steady_clock::time_point epoch =
      std::chrono::steady_clock::now();
  std::atomic_bool wallets_initialized{false};
  TransactionObservationTracker transactions;
  std::shared_ptr<LiveWalletWorkloadRegistry> wallet_workloads;
  std::shared_ptr<LiveBlockGenerationWorkloadRegistry> block_workloads;
  std::shared_ptr<LiveWaitUntilHeightWorkloadRegistry> height_workloads;
  std::shared_ptr<LiveWaitForPeersWorkloadRegistry> peer_workloads;
  std::shared_ptr<LiveInstrumentationRegistry> instrumentation =
      MakeLiveInstrumentationRegistry();
  RuntimeNodeAdditionDependencies addition{
      network_mutex, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}};
  RuntimeNodeRemovalDependencies removal{
      network_mutex, {}, {}, {}, {}, {}, {}, {}};
  RuntimeNodeReplacementDependencies replacement{
      network_mutex, resource_mutex, {}, {}, {}, {}, {}};
  std::atomic<std::shared_ptr<McpLiveRoleService>> roles;
  std::unique_ptr<McpLiveApplication> application;
  std::unique_ptr<SimulationCommandProcessor> processor;
  std::unique_ptr<McpDispatcher> dispatcher;
};

class PauseAfterWrites {
 public:
  PauseAfterWrites() {
    SetResourceLimitsAppliedHookForTest([state = state_] {
      state->entered = true;
      Await([&] { return state->released.load(); });
    });
  }
  ~PauseAfterWrites() { Release(); }
  void Wait() {
    Await([&] { return state_->entered.load(); });
  }
  void Release() { state_->released = true; }

 private:
  struct State {
    std::atomic_bool entered{false};
    std::atomic_bool released{false};
  };
  std::shared_ptr<State> state_ = std::make_shared<State>();
};

}  // namespace

BOOST_AUTO_TEST_CASE(operator_resource_mutation_cancels_before_first_write) {
  for (const bool profile : {false, true}) {
    ResourceCommands commands;
    if (!commands.run.prepared) {
      return;
    }
    std::unique_lock<std::mutex> lock(commands.resource_mutex);
    const auto submitted = commands.Submit(profile);
    Await([&] {
      return std::filesystem::exists(commands.events_path) &&
             ReadText(commands.events_path).find("operator_command_started") !=
                 std::string::npos;
    });
    commands.Cancel(submitted);
    lock.unlock();
    BOOST_TEST(commands.Terminal(submitted).at("state").as_string() ==
               "cancelled");
    commands.CheckState(commands.initial, "original");
    BOOST_CHECK(commands.EventKinds() ==
                std::vector<std::string>{"operator_command_started"});
    BOOST_TEST(!commands.run_stop_requested.load());
  }
}

BOOST_AUTO_TEST_CASE(operator_resource_mutation_finishes_after_cancellation) {
  for (const bool profile : {false, true}) {
    ResourceCommands commands;
    if (!commands.run.prepared) {
      return;
    }
    PauseAfterWrites pause;
    const auto submitted = commands.Submit(profile);
    pause.Wait();
    commands.Cancel(submitted);
    // Cover application/TUI shutdown too; neither source interrupts a commit.
    commands.command_stop.request_stop();
    std::this_thread::sleep_for(350ms);
    BOOST_TEST(commands.Inspect(submitted).at("state").as_string() ==
               "cancelling");
    BOOST_TEST(!commands.run_stop_requested.load());
    pause.Release();
    BOOST_TEST(commands.Terminal(submitted).at("state").as_string() ==
               "succeeded");
    commands.CheckState(commands.desired, profile ? "changed" : "");
    const std::vector<std::string> expected{
        "operator_command_started",
        profile ? "resource_profile_updated" : "resource_limits_updated",
        "operator_command_completed"};
    BOOST_CHECK(commands.EventKinds() == expected);
  }
}

BOOST_AUTO_TEST_CASE(operator_resource_mutation_rolls_back_real_write_failure) {
  for (const bool profile : {false, true}) {
    ResourceCommands commands;
    if (!commands.run.prepared) {
      return;
    }
    // Linux rejects a finite CPU quota below 1000us, after memory.high changed.
    commands.desired.cpu_quota_us = 1U;
    commands.options->resource_profiles.at("changed") = commands.desired;
    const auto terminal = commands.Terminal(commands.Submit(profile));
    BOOST_TEST(terminal.at("state").as_string() == "failed");
    BOOST_TEST(boost::json::serialize(terminal).find("cpu.max") !=
               std::string::npos);
    commands.CheckState(commands.initial, "original");
    const std::vector<std::string> expected{"operator_command_started",
                                            "operator_command_failed"};
    BOOST_CHECK(commands.EventKinds() == expected);
    BOOST_TEST(!commands.run_stop_requested.load());
  }
}
