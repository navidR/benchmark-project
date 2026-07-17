cmake_minimum_required(VERSION 3.20)

project(blockchain_benchmark_project LANGUAGES C CXX)

# CMake 4 requires an explicit compatibility floor for libyaml's release
# CMakeLists, which still declares compatibility with CMake 2.8.
set(CMAKE_POLICY_VERSION_MINIMUM 3.5)

include(CTest)
include(ExternalProject)

set(BOOST_INCLUDE_LIBRARIES
  asio
  beast
  json
  log
  program_options
  random
  system
  test
  url
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Boost.Regex can discover optional ICU support; the simulator does not vendor
# ICU, so keep the vendored Boost build from creating system-ICU targets.
set(CMAKE_DISABLE_FIND_PACKAGE_ICU ON)
add_subdirectory(third_party/boost EXCLUDE_FROM_ALL)

set(BLOCKCHAIN_BENCHMARK_PROJECT_BUILD_TESTING "${BUILD_TESTING}")
set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/libyaml EXCLUDE_FROM_ALL)
set(BUILD_TESTING "${BLOCKCHAIN_BENCHMARK_PROJECT_BUILD_TESTING}" CACHE BOOL "" FORCE)
add_library(yaml::yaml ALIAS yaml)

set(LIBMNL_GENERATED_INCLUDE "${CMAKE_CURRENT_BINARY_DIR}/generated/libmnl")
file(MAKE_DIRECTORY "${LIBMNL_GENERATED_INCLUDE}")
file(WRITE "${LIBMNL_GENERATED_INCLUDE}/config.h"
  "#pragma once\n"
  "#define HAVE_VISIBILITY_HIDDEN 1\n"
)

add_library(vendored_libmnl STATIC
  third_party/libmnl/src/attr.c
  third_party/libmnl/src/callback.c
  third_party/libmnl/src/nlmsg.c
  third_party/libmnl/src/socket.c
)
add_library(libmnl::libmnl ALIAS vendored_libmnl)
target_include_directories(vendored_libmnl
  SYSTEM PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/libmnl/include"
  PRIVATE
    "${LIBMNL_GENERATED_INCLUDE}"
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/libmnl/src"
)
set_target_properties(vendored_libmnl PROPERTIES
  OUTPUT_NAME mnl
  C_VISIBILITY_PRESET hidden
)

set(LIBPERF_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/third_party/libperf")
if(CMAKE_SYSTEM_PROCESSOR MATCHES "^(x86_64|amd64|i[3-6]86)$")
  set(LIBPERF_ARCH x86)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(aarch64|arm64)$")
  set(LIBPERF_ARCH arm64)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^arm")
  set(LIBPERF_ARCH arm)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^(ppc|ppc64|ppc64le|powerpc.*)$")
  set(LIBPERF_ARCH powerpc)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^riscv")
  set(LIBPERF_ARCH riscv)
elseif(CMAKE_SYSTEM_PROCESSOR MATCHES "^s390")
  set(LIBPERF_ARCH s390)
else()
  message(FATAL_ERROR
    "vendored libperf has no architecture mapping for ${CMAKE_SYSTEM_PROCESSOR}"
  )
endif()

set(LIBPERF_INCLUDE_DIRS
  "${LIBPERF_ROOT}/tools/lib/perf/include"
  "${LIBPERF_ROOT}/tools/lib"
  "${LIBPERF_ROOT}/tools/include"
  "${LIBPERF_ROOT}/tools/arch/${LIBPERF_ARCH}/include"
  "${LIBPERF_ROOT}/tools/arch/${LIBPERF_ARCH}/include/uapi"
  "${LIBPERF_ROOT}/tools/include/uapi"
)

add_library(vendored_libapi STATIC
  third_party/libperf/tools/lib/api/cpu.c
  third_party/libperf/tools/lib/api/debug.c
  third_party/libperf/tools/lib/api/fd/array.c
  third_party/libperf/tools/lib/api/fs/cgroup.c
  third_party/libperf/tools/lib/api/fs/fs.c
  third_party/libperf/tools/lib/api/fs/tracing_path.c
  third_party/libperf/tools/lib/str_error_r.c
)
target_compile_features(vendored_libapi PRIVATE c_std_99)
target_compile_definitions(vendored_libapi PRIVATE
  _LARGEFILE64_SOURCE
  _FILE_OFFSET_BITS=64
)
target_include_directories(vendored_libapi SYSTEM PRIVATE
  "${LIBPERF_ROOT}/tools/lib/api"
  "${LIBPERF_ROOT}/tools/include"
)
set_target_properties(vendored_libapi PROPERTIES
  C_EXTENSIONS ON
  POSITION_INDEPENDENT_CODE ON
)

add_library(vendored_libperf STATIC
  third_party/libperf/tools/lib/perf/core.c
  third_party/libperf/tools/lib/perf/cpumap.c
  third_party/libperf/tools/lib/perf/evlist.c
  third_party/libperf/tools/lib/perf/evsel.c
  third_party/libperf/tools/lib/perf/lib.c
  third_party/libperf/tools/lib/perf/mmap.c
  third_party/libperf/tools/lib/perf/threadmap.c
  third_party/libperf/tools/lib/perf/xyarray.c
  third_party/libperf/tools/lib/zalloc.c
)
add_library(libperf::libperf ALIAS vendored_libperf)
target_compile_features(vendored_libperf PRIVATE c_std_99)
target_include_directories(vendored_libperf SYSTEM PRIVATE
  ${LIBPERF_INCLUDE_DIRS}
)
target_include_directories(vendored_libperf SYSTEM INTERFACE
  "${LIBPERF_ROOT}/tools/lib/perf/include"
  "${LIBPERF_ROOT}/tools/include/uapi"
)
target_link_libraries(vendored_libperf PUBLIC vendored_libapi)
set_target_properties(vendored_libperf PROPERTIES
  C_EXTENSIONS ON
  C_VISIBILITY_PRESET hidden
  POSITION_INDEPENDENT_CODE ON
  OUTPUT_NAME perf
)

find_program(MAKE_EXECUTABLE make REQUIRED)
set(NCURSES_INSTALL_DIR
  "${CMAKE_CURRENT_BINARY_DIR}/third_party/ncurses/install"
)
set(NCURSES_INCLUDE_DIR "${NCURSES_INSTALL_DIR}/include")
set(NCURSES_LIBRARY "${NCURSES_INSTALL_DIR}/lib/libncursesw.a")
file(MAKE_DIRECTORY "${NCURSES_INCLUDE_DIR}")

ExternalProject_Add(vendored_ncurses_project
  SOURCE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/third_party/ncurses"
  BINARY_DIR "${CMAKE_CURRENT_BINARY_DIR}/third_party/ncurses/build"
  INSTALL_DIR "${NCURSES_INSTALL_DIR}"
  CONFIGURE_COMMAND
    <SOURCE_DIR>/configure
    CC=${CMAKE_C_COMPILER}
    --prefix=<INSTALL_DIR>
    --with-normal
    --without-shared
    --without-debug
    --without-ada
    --without-cxx-binding
    --without-tests
    --without-manpages
    --enable-widec
    --enable-ext-colors
    --disable-db-install
    --with-fallbacks=xterm,xterm-256color,screen,tmux,linux,vt100
  BUILD_COMMAND "${MAKE_EXECUTABLE}" -j16
  INSTALL_COMMAND "${MAKE_EXECUTABLE}" install
  BUILD_BYPRODUCTS "${NCURSES_LIBRARY}"
)

add_library(vendored_ncursesw STATIC IMPORTED GLOBAL)
add_library(ncurses::ncursesw ALIAS vendored_ncursesw)
set_target_properties(vendored_ncursesw PROPERTIES
  IMPORTED_LOCATION "${NCURSES_LIBRARY}"
  INTERFACE_INCLUDE_DIRECTORIES "${NCURSES_INCLUDE_DIR}"
  INTERFACE_SYSTEM_INCLUDE_DIRECTORIES "${NCURSES_INCLUDE_DIR}"
)
add_dependencies(vendored_ncursesw vendored_ncurses_project)

function(collect_directory_targets output directory)
  get_property(targets DIRECTORY "${directory}" PROPERTY BUILDSYSTEM_TARGETS)
  get_property(subdirectories DIRECTORY "${directory}" PROPERTY SUBDIRECTORIES)
  foreach(subdirectory IN LISTS subdirectories)
    collect_directory_targets(child_targets "${subdirectory}")
    list(APPEND targets ${child_targets})
  endforeach()
  set(${output} ${targets} PARENT_SCOPE)
endfunction()

function(mark_target_interface_includes_system target)
  get_target_property(include_dirs "${target}" INTERFACE_INCLUDE_DIRECTORIES)
  if(include_dirs)
    set_property(TARGET "${target}" APPEND PROPERTY
      INTERFACE_SYSTEM_INCLUDE_DIRECTORIES ${include_dirs}
    )
  endif()
endfunction()

collect_directory_targets(boost_targets "${CMAKE_CURRENT_SOURCE_DIR}/third_party/boost")
foreach(boost_target IN LISTS boost_targets)
  mark_target_interface_includes_system("${boost_target}")
endforeach()
mark_target_interface_includes_system(yaml)

add_executable(bbp
  src/block_production_policy.cpp
  src/capability.cpp
  src/cgroup.cpp
  src/chain_kind.cpp
  src/default_peer_topology.cpp
  src/drivers/chain_driver.cpp
  src/drivers/chain_command_executor.cpp
  src/drivers/chain_driver_registry.cpp
  src/drivers/firo_driver.cpp
  src/http_client.cpp
  src/log_tail.cpp
  src/log_level.cpp
  src/log_view.cpp
  src/logging.cpp
  src/main.cpp
  src/metric_sparkline.cpp
  src/mining_difficulty.cpp
  src/network.cpp
  src/network_rule_pane.cpp
  src/network_allocation_lock.cpp
  src/node_artifact_inventory.cpp
  src/node_file_pane.cpp
  src/node_log_collector.cpp
  src/node_log_pane.cpp
  src/operator_command_status.cpp
  src/peer_count_policy.cpp
  src/peer_connectivity_controller.cpp
  src/peer_list_pane.cpp
  src/perf_counter.cpp
  src/perf_counter_target_resolver.cpp
  src/periodic_metrics_collector.cpp
  src/positive_duration.cpp
  src/process.cpp
  src/process_control_config.cpp
  src/probabilistic_block_scheduler.cpp
  src/run_report.cpp
  src/runtime_peer_topology.cpp
  src/resource_limit_patch.cpp
  src/scheduled_scenario_event.cpp
  src/simulation_command.cpp
  src/simulation_command_processor.cpp
  src/simulation_command_queue.cpp
  src/simulation_event_kind.cpp
  src/simulation_registry.cpp
  src/simulation_network_address_plan.cpp
  src/simulation_time_scale.cpp
  src/signal_stop_monitor.cpp
  src/simulator_app.cpp
  src/tui.cpp
  src/tui_command_parser.cpp
  src/util.cpp
  src/wallet_funding_strategy.cpp
  src/wallet_transaction_plan.cpp
  src/yaml_helpers.cpp
)

target_include_directories(bbp PRIVATE include)
target_link_libraries(bbp PRIVATE
  Boost::asio
  Boost::beast
  Boost::json
  Boost::log
  Boost::log_setup
  Boost::program_options
  Boost::random
  Boost::system
  Boost::url
  libperf::libperf
  libmnl::libmnl
  ncurses::ncursesw
  yaml::yaml
)
target_compile_options(bbp PRIVATE
  -Wall
  -Wextra
  -Wpedantic
  -Wconversion
  -Werror
)

if(BUILD_TESTING)
  add_executable(bbp-unit-tests
    tests/unit/block_production_policy_tests.cpp
    tests/unit/capability_tests.cpp
    tests/unit/cgroup_tests.cpp
    tests/unit/chain_kind_tests.cpp
    tests/unit/chain_wallet_transaction_tests.cpp
    tests/unit/default_peer_topology_tests.cpp
    tests/unit/firo_driver_tests.cpp
    tests/unit/log_tail_tests.cpp
    tests/unit/log_level_tests.cpp
    tests/unit/logging_tests.cpp
    tests/unit/metric_sparkline_tests.cpp
    tests/unit/network_tests.cpp
    tests/unit/network_rule_pane_tests.cpp
    tests/unit/node_artifact_inventory_tests.cpp
    tests/unit/node_file_pane_tests.cpp
    tests/unit/simulation_network_address_plan_tests.cpp
    tests/unit/node_log_pane_tests.cpp
    tests/unit/node_runtime_tests.cpp
    tests/unit/operator_command_status_tests.cpp
    tests/unit/peer_connectivity_controller_tests.cpp
    tests/unit/peer_list_pane_tests.cpp
    tests/unit/perf_counter_tests.cpp
    tests/unit/perf_counter_target_resolver_tests.cpp
    tests/unit/no_shellout_tests.cpp
    tests/unit/periodic_metrics_collector_tests.cpp
    tests/unit/positive_duration_tests.cpp
    tests/unit/process_tests.cpp
    tests/unit/process_control_config_tests.cpp
    tests/unit/probabilistic_block_scheduler_tests.cpp
    tests/unit/run_report_tests.cpp
    tests/unit/runtime_peer_topology_tests.cpp
    tests/unit/resource_limit_patch_tests.cpp
    tests/unit/scheduled_scenario_event_tests.cpp
    tests/unit/simulation_command_processor_tests.cpp
    tests/unit/simulation_command_queue_tests.cpp
    tests/unit/simulation_command_tests.cpp
    tests/unit/simulation_event_kind_tests.cpp
    tests/unit/simulation_registry_tests.cpp
    tests/unit/simulation_time_scale_tests.cpp
    tests/unit/signal_stop_monitor_tests.cpp
    tests/unit/tui_command_parser_tests.cpp
    tests/unit/util_tests.cpp
    tests/unit/wallet_funding_strategy_tests.cpp
    tests/unit/wallet_transaction_plan_tests.cpp
    tests/unit/wallet_transfer_strategy_tests.cpp
    tests/unit/workload_kind_tests.cpp
    src/capability.cpp
    src/cgroup.cpp
    src/chain_kind.cpp
    src/block_production_policy.cpp
    src/default_peer_topology.cpp
    src/simulation_command.cpp
    src/simulation_event_kind.cpp
    src/drivers/chain_driver.cpp
    src/drivers/chain_driver_registry.cpp
    src/drivers/firo_driver.cpp
    src/http_client.cpp
    src/log_tail.cpp
    src/log_level.cpp
    src/log_view.cpp
    src/logging.cpp
    src/metric_sparkline.cpp
    src/mining_difficulty.cpp
    src/network.cpp
    src/network_rule_pane.cpp
    src/network_allocation_lock.cpp
    src/node_artifact_inventory.cpp
    src/node_file_pane.cpp
    src/node_log_pane.cpp
    src/operator_command_status.cpp
    src/peer_count_policy.cpp
    src/peer_connectivity_controller.cpp
    src/peer_list_pane.cpp
    src/perf_counter.cpp
    src/perf_counter_target_resolver.cpp
    src/periodic_metrics_collector.cpp
    src/positive_duration.cpp
    src/process.cpp
    src/process_control_config.cpp
    src/probabilistic_block_scheduler.cpp
    src/resource_limit_patch.cpp
    src/run_report.cpp
    src/runtime_peer_topology.cpp
    src/scheduled_scenario_event.cpp
    src/simulation_command_processor.cpp
    src/simulation_command_queue.cpp
    src/simulation_registry.cpp
    src/simulation_network_address_plan.cpp
    src/simulation_time_scale.cpp
    src/signal_stop_monitor.cpp
    src/tui_command_parser.cpp
    src/util.cpp
    src/wallet_funding_strategy.cpp
    src/wallet_transaction_plan.cpp
  )
  target_include_directories(bbp-unit-tests PRIVATE include)
  target_compile_definitions(bbp-unit-tests PRIVATE
    BBP_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
  )
  target_link_libraries(bbp-unit-tests PRIVATE
    Boost::asio
    Boost::beast
    Boost::json
    Boost::log
    Boost::log_setup
    Boost::random
    Boost::unit_test_framework
    Boost::system
    Boost::url
    libperf::libperf
    libmnl::libmnl
  )
  target_compile_options(bbp-unit-tests PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
    -Werror
  )
  add_test(NAME bbp-unit-tests COMMAND bbp-unit-tests)

  function(add_cli_failure_test name scenario regex)
    add_test(NAME "${name}"
      COMMAND "${CMAKE_COMMAND}"
        "-DTEST_COMMAND=$<TARGET_FILE:bbp>"
        "-DSCENARIO=${CMAKE_CURRENT_SOURCE_DIR}/${scenario}"
        "-DREGEX=${regex}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/expect_failure.cmake"
    )
  endfunction()

  function(add_cli_failure_test_with_option name option scenario regex)
    add_test(NAME "${name}"
      COMMAND "${CMAKE_COMMAND}"
        "-DTEST_COMMAND=$<TARGET_FILE:bbp>"
        "-DSCENARIO_OPTION=${option}"
        "-DSCENARIO=${CMAKE_CURRENT_SOURCE_DIR}/${scenario}"
        "-DREGEX=${regex}"
        -P "${CMAKE_CURRENT_SOURCE_DIR}/tests/cmake/expect_failure.cmake"
    )
  endfunction()

  add_test(NAME bbp-valid-yaml-scenario
    COMMAND bbp
      --scenario-yaml
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-yaml-scenario.yaml"
      --probe-capabilities
  )
  add_test(NAME bbp-canonical-json-scenario
    COMMAND bbp
      --scenario
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-role-topology-scenario.json"
      --chain firo
      --node-binary /tmp/not-used-by-probe
      --probe-capabilities
  )
  add_test(NAME bbp-canonical-yaml-scenario
    COMMAND bbp
      --scenario
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-yaml-scenario.yaml"
      --chain firo
      --node-binary /tmp/not-used-by-probe
      --probe-capabilities
  )
  add_test(NAME bbp-valid-peer-churn-scenario
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-peer-churn-scenario.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-raw-transaction-scenario
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-raw-transaction-scenario.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-role-topology-scenario
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-role-topology-scenario.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-peer-connectivity-topology
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-peer-connectivity-topology.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-logical-topology
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-logical-topology-scenario.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-directional-edge-condition
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-directional-edge-condition.json"
      --probe-capabilities
  )
  add_cli_failure_test(
    bbp-invalid-directional-edge-condition-without-isolation
    tests/scenarios/invalid-directional-edge-condition-without-isolation.json
    "network runtime options require --isolate-network"
  )
  add_cli_failure_test(
    bbp-invalid-directional-edge-condition-loss
    tests/scenarios/invalid-directional-edge-condition-loss.json
    "netem loss basis points must be 0..10000"
  )
  add_cli_failure_test(
    bbp-invalid-directional-edge-condition-limit
    tests/scenarios/invalid-directional-edge-condition-limit.json
    "netem packet limit must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-directional-edge-condition-latency-alias
    tests/scenarios/invalid-directional-edge-condition-latency-alias.json
    "scenario topology edge latency_ms and delay_ms must match"
  )
  add_test(NAME bbp-valid-wallet-transactions-scenario
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-wallet-transactions-scenario.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-wallet-random-transactions-scenario
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-wallet-random-transactions-scenario.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-wallet-distribution-strategies
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-wallet-distribution-strategies.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-resource-pressure-scenario
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-resource-pressure-scenario.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-network-control-workloads
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-network-control-workloads.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-cgroup-resource-controls
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-cgroup-resource-controls.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-named-profiles
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-named-profiles.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-named-profiles-yaml
    COMMAND bbp
      --scenario-yaml
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-named-profiles.yaml"
      --probe-capabilities
  )
  add_cli_failure_test(
    bbp-invalid-unknown-resource-profile
    tests/scenarios/invalid-unknown-resource-profile.json
    "references unknown resource profile: missing"
  )
  add_cli_failure_test(
    bbp-invalid-unknown-network-profile
    tests/scenarios/invalid-unknown-network-profile.json
    "references unknown network profile: missing"
  )
  add_cli_failure_test(
    bbp-invalid-duplicate-node-id
    tests/scenarios/invalid-duplicate-node-id.json
    "scenario nodes contains duplicate id: firo-a"
  )
  add_cli_failure_test(
    bbp-invalid-node-array-count
    tests/scenarios/invalid-node-array-count.json
    "scenario node_count must match scenario nodes array size"
  )
  add_cli_failure_test(
    bbp-invalid-node-id
    tests/scenarios/invalid-node-id.json
    "scenario node id contains an unsafe character"
  )
  add_cli_failure_test(
    bbp-invalid-resource-profile-bytes
    tests/scenarios/invalid-resource-profile-bytes.json
    "resource profile memory_max overflows uint64 bytes"
  )
  add_cli_failure_test(
    bbp-invalid-resource-profile-cpu
    tests/scenarios/invalid-resource-profile-cpu.json
    "resource profile cpu_quota must contain exactly quota-or-max and period"
  )
  add_cli_failure_test(
    bbp-invalid-network-profile-loss
    tests/scenarios/invalid-network-profile-loss.json
    "loss_percent must use at most 0.01 percent resolution"
  )
  add_cli_failure_test(
    bbp-invalid-network-profile-loss-range
    tests/scenarios/invalid-network-profile-loss-range.json
    "loss_percent must be in 0..100"
  )
  add_cli_failure_test(
    bbp-invalid-network-profile-loss-conflict
    tests/scenarios/invalid-network-profile-loss-conflict.json
    "loss_percent and loss_basis_points must not both be specified"
  )
  add_cli_failure_test(
    bbp-invalid-resource-profile-memory-conflict
    tests/scenarios/invalid-resource-profile-memory-conflict.json
    "specifies both memory_max and memory_max_bytes"
  )
  add_cli_failure_test(
    bbp-invalid-resource-profile-cpu-conflict
    tests/scenarios/invalid-resource-profile-cpu-conflict.json
    "CPU aliases conflict with cpu_quota_us or cpu_period_us"
  )
  add_cli_failure_test(
    bbp-invalid-node-role-topology
    tests/scenarios/invalid-node-role-topology.json
    "scenario nodes roles must match topology wallet_nodes and miner_nodes"
  )
  add_cli_failure_test(
    bbp-invalid-node-chain
    tests/scenarios/invalid-node-chain.json
    "chain must match the active chain"
  )
  add_cli_failure_test(
    bbp-invalid-network-profile-conflict
    tests/scenarios/invalid-network-profile-conflict.json
    "cannot combine network.profile with network.node_conditions"
  )
  add_test(NAME bbp-valid-scheduled-events-scenario
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-scheduled-events-scenario.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-canonical-simulation
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-canonical-simulation.json"
      --probe-capabilities
  )
  add_test(NAME bbp-valid-canonical-simulation-yaml
    COMMAND bbp
      --scenario-yaml
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-canonical-simulation.yaml"
      --probe-capabilities
  )
  add_cli_failure_test(
    bbp-invalid-simulation-duration-samples
    tests/scenarios/invalid-simulation-duration-samples.json
    "simulation duration and metrics_sample_count must not be combined"
  )
  add_cli_failure_test(
    bbp-invalid-simulation-event-at-duration
    tests/scenarios/invalid-simulation-event-at-duration.json
    "scheduled event time must be less than simulation duration"
  )
  add_cli_failure_test(
    bbp-invalid-simulation-metrics-alias
    tests/scenarios/invalid-simulation-metrics-alias.json
    "simulation.metrics_interval and tick_interval are aliases"
  )
  add_cli_failure_test(
    bbp-invalid-simulation-name
    tests/scenarios/invalid-simulation-name.json
    "simulation name contains an unsafe character"
  )
  add_cli_failure_test(
    bbp-invalid-simulation-output-alias
    tests/scenarios/invalid-simulation-output-alias.json
    "simulation.output_dir and top-level output_dir must not be combined"
  )
  add_cli_failure_test(
    bbp-invalid-simulation-scaled-duration
    tests/scenarios/invalid-simulation-scaled-duration.json
    "scaled simulation duration exceeds milliseconds range"
  )
  add_cli_failure_test(
    bbp-invalid-simulation-time-scale
    tests/scenarios/invalid-simulation-time-scale.json
    "simulation time_scale supports at most six decimal places"
  )
  add_test(NAME bbp-valid-profile-switch-events
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-profile-switch-events.json"
      --probe-capabilities
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-unknown-resource-profile
    tests/scenarios/invalid-profile-switch-unknown-resource-profile.json
    "scenario set_resource_profile references unknown profile: missing"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-unknown-network-profile
    tests/scenarios/invalid-profile-switch-unknown-network-profile.json
    "scenario set_network_profile references unknown profile: missing"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-missing-nodes
    tests/scenarios/invalid-profile-switch-missing-nodes.json
    "scenario set_resource_profile requires nodes"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-empty-nodes
    tests/scenarios/invalid-profile-switch-empty-nodes.json
    "scenario set_resource_profile nodes must not be empty"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-unknown-node
    tests/scenarios/invalid-profile-switch-unknown-node.json
    "scenario set_resource_profile references unknown node ID: firo-b"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-duplicate-node
    tests/scenarios/invalid-profile-switch-duplicate-node.json
    "scenario set_resource_profile nodes contains duplicate ID: firo-a"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-node-type
    tests/scenarios/invalid-profile-switch-node-type.json
    "scenario set_resource_profile nodes entries must be node ID strings"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-role
    tests/scenarios/invalid-profile-switch-role.json
    "profile switch role selector must be role:base, role:node, role:wallet, or role:miner"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-empty-role
    tests/scenarios/invalid-profile-switch-empty-role.json
    "scenario set_resource_profile role selector resolved no nodes"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-missing-profile
    tests/scenarios/invalid-profile-switch-missing-profile.json
    "missing or invalid string JSON field: profile"
  )
  add_cli_failure_test(
    bbp-invalid-profile-switch-unsafe-profile
    tests/scenarios/invalid-profile-switch-unsafe-profile.json
    "profile switch name contains an unsafe character"
  )
  add_test(NAME bbp-valid-topology-edge-events
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-topology-edge-events.json"
      --probe-capabilities
  )
  add_cli_failure_test(
    bbp-invalid-topology-edge-inactive-condition
    tests/scenarios/invalid-topology-edge-inactive-condition.json
    "cannot set a condition on an inactive runtime topology edge"
  )
  add_cli_failure_test(
    bbp-invalid-topology-edge-already-active
    tests/scenarios/invalid-topology-edge-already-active.json
    "runtime topology edge is already active"
  )
  add_cli_failure_test(
    bbp-invalid-topology-edge-condition-without-isolation
    tests/scenarios/invalid-topology-edge-condition-without-isolation.json
    "network runtime options require --isolate-network"
  )
  add_cli_failure_test(
    bbp-invalid-topology-edge-condition-on-activate
    tests/scenarios/invalid-topology-edge-condition-on-activate.json
    "scenario activate_edge does not accept condition fields"
  )
  add_cli_failure_test(
    bbp-invalid-topology-edge-peer-policy
    tests/scenarios/invalid-topology-edge-peer-policy.json
    "peer policy minimum exceeds allowed logical peers after topology edge action"
  )
  add_cli_failure_test(
    bbp-invalid-topology-edge-timeout
    tests/scenarios/invalid-topology-edge-timeout.json
    "scenario topology edge action timeout_sec must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-logical-topology-latency-matrix
    tests/scenarios/invalid-logical-topology-latency-matrix.json
    "latency matrix column count must match node count"
  )
  add_cli_failure_test(
    bbp-invalid-logical-topology-peer-policy
    tests/scenarios/invalid-logical-topology-peer-policy.json
    "min_peer_count exceeds the node's eligible logical topology peers"
  )
  add_cli_failure_test(
    bbp-invalid-scheduled-events-not-array
    tests/scenarios/invalid-scheduled-events-not-array.json
    "scenario events must be a JSON array"
  )
  add_cli_failure_test(
    bbp-invalid-scheduled-event-entry
    tests/scenarios/invalid-scheduled-event-entry.json
    "scenario events entries must be JSON objects"
  )
  add_cli_failure_test(
    bbp-invalid-scheduled-event-missing-at
    tests/scenarios/invalid-scheduled-event-missing-at.json
    "missing or invalid string JSON field: at"
  )
  add_cli_failure_test(
    bbp-invalid-scheduled-event-missing-action
    tests/scenarios/invalid-scheduled-event-missing-action.json
    "missing or invalid string JSON field: action"
  )
  add_cli_failure_test(
    bbp-invalid-scheduled-event-time
    tests/scenarios/invalid-scheduled-event-time.json
    "duration must contain a positive integer and explicit unit"
  )
  add_cli_failure_test(
    bbp-invalid-scheduled-event-action
    tests/scenarios/invalid-scheduled-event-action.json
    "unsupported scheduled event action: unknown_action"
  )
  add_cli_failure_test(
    bbp-invalid-scheduled-event-type
    tests/scenarios/invalid-scheduled-event-type.json
    "scenario events entries use action, not type"
  )
  add_cli_failure_test(
    bbp-invalid-scheduled-event-node
    tests/scenarios/invalid-scheduled-event-node.json
    "scenario restart_node workload node must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-scheduled-event-clock-overflow
    tests/scenarios/invalid-scheduled-event-clock-overflow.json
    "scheduled event time exceeds monotonic clock range"
  )
  add_cli_failure_test(
    bbp-invalid-checkpoint-name
    tests/scenarios/invalid-checkpoint-name.json
    "checkpoint name contains an unsafe character"
  )
  add_cli_failure_test(
    bbp-invalid-legacy-generate-blocks
    tests/scenarios/invalid-legacy-generate-blocks.json
    "scenario generate_blocks was removed"
  )
  add_cli_failure_test(
    bbp-invalid-block-production-without-miner
    tests/scenarios/invalid-block-production-without-miner.json
    "enabled block production requires at least one configured miner"
  )
  add_test(NAME bbp-benchmark-root-option
    COMMAND bbp
      --benchmark-root "${CMAKE_CURRENT_BINARY_DIR}/benchmark-root-option"
      --probe-capabilities
  )
  add_test(NAME bbp-benchmark-root-output-dir-conflict
    COMMAND bbp
      --benchmark-root "${CMAKE_CURRENT_BINARY_DIR}/benchmark-root-option"
      --output-dir "${CMAKE_CURRENT_BINARY_DIR}/legacy-output-dir"
      --probe-capabilities
  )
  set_tests_properties(bbp-benchmark-root-output-dir-conflict
    PROPERTIES
      WILL_FAIL TRUE
  )
  add_test(NAME bbp-integrated-tui-once
    COMMAND "${CMAKE_COMMAND}" -E env TERM=xterm
      $<TARGET_FILE:bbp>
      --run "${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures/tui-run"
      --once
  )
  add_test(NAME bbp-report-run-id-under-benchmark-root
    COMMAND bbp
      --benchmark-root "${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures"
      --report-run tui-run
  )
  add_test(NAME bbp-tui-run-id-under-benchmark-root
    COMMAND "${CMAKE_COMMAND}" -E env TERM=xterm
      $<TARGET_FILE:bbp>
      --benchmark-root "${CMAKE_CURRENT_SOURCE_DIR}/tests/fixtures"
      --run tui-run
      --once
  )
  add_test(NAME bbp-cleanup-run-id-conflict
    COMMAND bbp
      --cleanup-run cleanup-id
      --run-id different-id
      --probe-capabilities
  )
  set_tests_properties(bbp-cleanup-run-id-conflict
    PROPERTIES
      WILL_FAIL TRUE
  )
  add_cli_failure_test(
    bbp-invalid-role-topology-miners
    tests/scenarios/invalid-role-topology-miners.json
    "scenario topology miner_node_count must be <= node_count"
  )
  add_cli_failure_test(
    bbp-invalid-role-topology-capacity
    tests/scenarios/invalid-role-topology-capacity.json
    "scenario topology wallet_node_count plus miner_node_count must be <= node_count when overlap is disabled"
  )
  add_cli_failure_test(
    bbp-invalid-peer-connectivity-duplicate-node
    tests/scenarios/invalid-peer-connectivity-duplicate-node.json
    "scenario topology.peer_connectivity contains duplicate node"
  )
  add_cli_failure_test(
    bbp-invalid-peer-connectivity-max-count
    tests/scenarios/invalid-peer-connectivity-max-count.json
    "scenario topology.peer_connectivity max_peer_count must be less than node_count"
  )
  add_cli_failure_test(
    bbp-invalid-peer-connectivity-range
    tests/scenarios/invalid-peer-connectivity-range.json
    "scenario topology.peer_connectivity min_peer_count must not exceed max_peer_count"
  )
  add_cli_failure_test(
    bbp-invalid-wait-height-missing-height
    tests/scenarios/invalid-wait-height-missing-height.json
    "missing or invalid uint64 JSON field: height"
  )
  add_cli_failure_test(
    bbp-invalid-wait-height-node
    tests/scenarios/invalid-wait-height-node.json
    "scenario wait_until_height workload node must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-wait-height-timeout
    tests/scenarios/invalid-wait-height-timeout.json
    "scenario wait_until_height timeout_sec must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-wait-height-plural-nodes
    tests/scenarios/invalid-wait-height-plural-nodes.json
    "current MVP wait_until_height workload uses node, not nodes"
  )
  add_cli_failure_test(
    bbp-invalid-wait-peers-missing-peer-count
    tests/scenarios/invalid-wait-peers-missing-peer-count.json
    "missing or invalid uint64 JSON field: peer_count"
  )
  add_cli_failure_test(
    bbp-invalid-wait-peers-node
    tests/scenarios/invalid-wait-peers-node.json
    "scenario wait_for_peers workload node must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-wait-peers-timeout
    tests/scenarios/invalid-wait-peers-timeout.json
    "scenario wait_for_peers timeout_sec must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-wait-peers-zero-count
    tests/scenarios/invalid-wait-peers-zero-count.json
    "scenario wait_for_peers peer_count must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-wait-peers-plural-nodes
    tests/scenarios/invalid-wait-peers-plural-nodes.json
    "current MVP wait_for_peers workload uses node, not nodes"
  )
  add_cli_failure_test(
    bbp-invalid-connect-peer-peer
    tests/scenarios/invalid-connect-peer-peer.json
    "scenario connect_peer workload peer must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-connect-peer-self
    tests/scenarios/invalid-connect-peer-self.json
    "scenario connect_peer workload node and peer must differ"
  )
  add_cli_failure_test(
    bbp-invalid-disconnect-peer-missing-peer
    tests/scenarios/invalid-disconnect-peer-missing-peer.json
    "missing or invalid uint32 JSON field: peer"
  )
  add_cli_failure_test(
    bbp-invalid-disconnect-peer-zero-timeout
    tests/scenarios/invalid-disconnect-peer-zero-timeout.json
    "scenario disconnect_peer timeout_sec must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-raw-transaction-missing-private-key
    tests/scenarios/invalid-raw-transaction-missing-private-key.json
    "missing or invalid string JSON field: source_private_key"
  )
  add_cli_failure_test(
    bbp-invalid-raw-transaction-funding-blocks
    tests/scenarios/invalid-raw-transaction-funding-blocks.json
    "scenario send_raw_transaction funding_blocks must be at least 101"
  )
  add_cli_failure_test(
    bbp-invalid-raw-transaction-same-address
    tests/scenarios/invalid-raw-transaction-same-address.json
    "scenario send_raw_transaction source_address and destination_address must differ"
  )
  add_cli_failure_test(
    bbp-invalid-raw-transaction-zero-amount
    tests/scenarios/invalid-raw-transaction-zero-amount.json
    "scenario send_raw_transaction amount must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-transactions-wallet-material
    tests/scenarios/invalid-wallet-transactions-wallet-material.json
    "scenario wallet_transactions workload must use topology.wallet_initialization instead of wallet keys or addresses"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-funding-strategy
    tests/scenarios/invalid-wallet-funding-strategy.json
    "scenario wallet_transactions funding_strategy must be round_robin or random"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-funding-threshold
    tests/scenarios/invalid-wallet-funding-threshold.json
    "scenario wallet_transactions funding_threshold must cover amount plus fee"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-transfer-strategy
    tests/scenarios/invalid-wallet-transfer-strategy.json
    "scenario wallet_transactions strategy must be round_robin, random, fanout, or hotspot"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-amount-shape
    tests/scenarios/invalid-wallet-amount-shape.json
    "invalid fixed-8 amount JSON field: amount"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-amount-distribution
    tests/scenarios/invalid-wallet-amount-distribution.json
    "amount distribution must be fixed or uniform"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-amount-distribution-missing-bound
    tests/scenarios/invalid-wallet-amount-distribution-missing-bound.json
    "amount distribution requires min and max"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-amount-distribution-field
    tests/scenarios/invalid-wallet-amount-distribution-field.json
    "amount distribution contains unsupported field: mean"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-amount-distribution-zero
    tests/scenarios/invalid-wallet-amount-distribution-zero.json
    "scenario wallet_transactions amount must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-amount-distribution-inverted
    tests/scenarios/invalid-wallet-amount-distribution-inverted.json
    "scenario wallet_transactions amount distribution min must be <= max"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-interval-shape
    tests/scenarios/invalid-wallet-interval-shape.json
    "interval must be a duration string"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-interval-distribution-missing-bound
    tests/scenarios/invalid-wallet-interval-distribution-missing-bound.json
    "interval distribution requires min and max"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-interval-distribution-inverted
    tests/scenarios/invalid-wallet-interval-distribution-inverted.json
    "scenario wallet_transactions interval distribution min must be <= max"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-selector-shape
    tests/scenarios/invalid-wallet-selector-shape.json
    "scenario wallet_transactions sender_wallets must be a JSON array"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-selector-range
    tests/scenarios/invalid-wallet-selector-range.json
    "scenario wallet_transactions sender_wallets values must be in 1..wallet_count"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-selector-duplicate
    tests/scenarios/invalid-wallet-selector-duplicate.json
    "scenario wallet_transactions sender_wallets contains a duplicate"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-fanout-missing-senders
    tests/scenarios/invalid-wallet-fanout-missing-senders.json
    "scenario wallet_transactions fanout requires sender_wallets"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-fanout-all-senders
    tests/scenarios/invalid-wallet-fanout-all-senders.json
    "scenario wallet_transactions fanout sender_wallets must leave at least one receiver"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-fanout-receivers
    tests/scenarios/invalid-wallet-fanout-receivers.json
    "scenario wallet_transactions fanout does not accept receiver_wallets"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-hotspot-missing-receivers
    tests/scenarios/invalid-wallet-hotspot-missing-receivers.json
    "scenario wallet_transactions hotspot requires receiver_wallets"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-hotspot-all-receivers
    tests/scenarios/invalid-wallet-hotspot-all-receivers.json
    "scenario wallet_transactions hotspot receiver_wallets must leave at least one sender"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-hotspot-senders
    tests/scenarios/invalid-wallet-hotspot-senders.json
    "scenario wallet_transactions hotspot does not accept sender_wallets"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-selector-strategy
    tests/scenarios/invalid-wallet-selector-strategy.json
    "scenario wallet_transactions wallet selectors require fanout or hotspot strategy"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-distribution-funding-threshold
    tests/scenarios/invalid-wallet-distribution-funding-threshold.json
    "scenario wallet_transactions funding_threshold must cover amount plus fee"
  )
  add_cli_failure_test(
    bbp-invalid-wallet-distribution-overflow
    tests/scenarios/invalid-wallet-distribution-overflow.json
    "scenario wallet_transactions amount plus fee overflows uint64"
  )
  add_cli_failure_test(
    bbp-invalid-restart-node-node
    tests/scenarios/invalid-restart-node-node.json
    "scenario restart_node workload node must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-restart-node-plural-nodes
    tests/scenarios/invalid-restart-node-plural-nodes.json
    "current MVP restart_node workload uses node, not nodes"
  )
  add_cli_failure_test(
    bbp-invalid-freeze-node-missing-duration
    tests/scenarios/invalid-freeze-node-missing-duration.json
    "missing or invalid uint32 JSON field: duration_ms"
  )
  add_cli_failure_test(
    bbp-invalid-freeze-node-node
    tests/scenarios/invalid-freeze-node-node.json
    "scenario freeze_node workload node must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-freeze-node-plural-nodes
    tests/scenarios/invalid-freeze-node-plural-nodes.json
    "current MVP freeze_node workload uses node, not nodes"
  )
  add_cli_failure_test(
    bbp-invalid-freeze-node-zero-duration
    tests/scenarios/invalid-freeze-node-zero-duration.json
    "scenario freeze_node duration_ms must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-update-resource-limits-empty
    tests/scenarios/invalid-update-resource-limits-empty.json
    "runtime resource update has no limit fields"
  )
  add_cli_failure_test(
    bbp-invalid-cgroup-cpu-weight
    tests/scenarios/invalid-cgroup-cpu-weight.json
    "--cpu-weight must be in 1..10000"
  )
  add_cli_failure_test(
    bbp-invalid-cgroup-io-device
    tests/scenarios/invalid-cgroup-io-device.json
    "block device id exceeds uint32"
  )
  add_cli_failure_test(
    bbp-invalid-cgroup-io-duplicate
    tests/scenarios/invalid-cgroup-io-duplicate.json
    "scenario resources.io_max contains a duplicate block device: 252:1"
  )
  add_cli_failure_test(
    bbp-invalid-cgroup-io-missing-field
    tests/scenarios/invalid-cgroup-io-missing-field.json
    "missing io.max field: write_operations_per_sec"
  )
  add_cli_failure_test(
    bbp-invalid-cgroup-io-zero
    tests/scenarios/invalid-cgroup-io-zero.json
    "read_bytes_per_sec must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-update-resource-limits-node
    tests/scenarios/invalid-update-resource-limits-node.json
    "scenario update_resource_limits workload node must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-update-resource-limits-plural-nodes
    tests/scenarios/invalid-update-resource-limits-plural-nodes.json
    "current MVP update_resource_limits workload uses node, not nodes"
  )
  add_cli_failure_test(
    bbp-invalid-resource-pressure-missing-duration
    tests/scenarios/invalid-resource-pressure-missing-duration.json
    "missing or invalid uint32 JSON field: duration_ms"
  )
  add_cli_failure_test(
    bbp-invalid-resource-pressure-zero-duration
    tests/scenarios/invalid-resource-pressure-zero-duration.json
    "scenario resource_pressure duration_ms must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-resource-pressure-node
    tests/scenarios/invalid-resource-pressure-node.json
    "scenario resource_pressure workload node must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-resource-pressure-plural-nodes
    tests/scenarios/invalid-resource-pressure-plural-nodes.json
    "current MVP resource_pressure workload uses node, not nodes"
  )
  add_cli_failure_test(
    bbp-invalid-partition-workload-requires-isolate
    tests/scenarios/invalid-partition-workload-requires-isolate.json
    "network runtime options require --isolate-network"
  )
  add_cli_failure_test(
    bbp-invalid-network-control-without-isolation
    tests/scenarios/invalid-network-control-without-isolation.json
    "network runtime options require --isolate-network"
  )
  add_cli_failure_test(
    bbp-invalid-network-condition-node
    tests/scenarios/invalid-network-condition-node.json
    "scenario set_network_condition workload node must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-network-condition-basis-points
    tests/scenarios/invalid-network-condition-basis-points.json
    "netem loss basis points must be 0..10000"
  )
  add_cli_failure_test(
    bbp-invalid-network-condition-limit
    tests/scenarios/invalid-network-condition-limit.json
    "netem packet limit must be greater than zero"
  )
  add_cli_failure_test(
    bbp-invalid-network-flow-node
    tests/scenarios/invalid-network-flow-node.json
    "scenario network flow workload node must be in 1..--nodes"
  )
  add_cli_failure_test(
    bbp-invalid-network-flow-port
    tests/scenarios/invalid-network-flow-port.json
    "network block rule dst_port must be 1..65535"
  )
  add_cli_failure_test(
    bbp-invalid-network-flow-address
    tests/scenarios/invalid-network-flow-address.json
    "invalid IPv4 network block destination: invalid"
  )
  add_cli_failure_test(
    bbp-invalid-partition-workload-node
    tests/scenarios/invalid-partition-workload-node.json
    "scenario partition_nodes workload group_b node must be in 1..2"
  )
  add_cli_failure_test(
    bbp-invalid-heal-partition-workload-overlap
    tests/scenarios/invalid-heal-partition-workload-overlap.json
    "partition groups must not contain the same node"
  )
  add_cli_failure_test_with_option(
    bbp-invalid-yaml-root-sequence
    "--scenario-yaml"
    tests/scenarios/invalid-yaml-root-sequence.yaml
    "--scenario-yaml root must be a YAML mapping"
  )
  add_cli_failure_test_with_option(
    bbp-invalid-yaml-quoted-nodes
    "--scenario-yaml"
    tests/scenarios/invalid-yaml-quoted-nodes.yaml
    "invalid uint32 JSON field: nodes"
  )
endif()

install(TARGETS bbp RUNTIME DESTINATION bin)
