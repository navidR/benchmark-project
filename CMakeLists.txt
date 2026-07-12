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

set(LIBCAP_GENERATED_INCLUDE "${CMAKE_CURRENT_BINARY_DIR}/generated/libcap")
file(MAKE_DIRECTORY "${LIBCAP_GENERATED_INCLUDE}")
file(STRINGS
  "${CMAKE_CURRENT_SOURCE_DIR}/third_party/libcap/libcap/include/uapi/linux/capability.h"
  LIBCAP_CAPABILITY_LINES
  REGEX "^#define[ \t]+CAP_[^ \t]+[ \t]+[0-9]+[ \t]*$"
)
set(LIBCAP_NAMES_BY_INDEX)
set(LIBCAP_CAP_NAME_SIZE 0)
foreach(capability_line IN LISTS LIBCAP_CAPABILITY_LINES)
  string(REGEX MATCH
    "^#define[ \t]+CAP_([^ \t]+)[ \t]+([0-9]+)[ \t]*$"
    capability_match
    "${capability_line}"
  )
  if(NOT capability_match)
    message(FATAL_ERROR
      "could not parse libcap capability line: ${capability_line}"
    )
  endif()
  string(TOLOWER "${CMAKE_MATCH_1}" capability_name)
  set(capability_index "${CMAKE_MATCH_2}")
  list(LENGTH LIBCAP_NAMES_BY_INDEX libcap_name_count)
  if(NOT capability_index EQUAL libcap_name_count)
    message(FATAL_ERROR
      "libcap capability indices must be contiguous: ${capability_line}"
    )
  endif()
  list(APPEND LIBCAP_NAMES_BY_INDEX "${capability_name}")
  string(LENGTH "${capability_name}" capability_name_size)
  if(capability_name_size GREATER LIBCAP_CAP_NAME_SIZE)
    set(LIBCAP_CAP_NAME_SIZE "${capability_name_size}")
  endif()
endforeach()
list(LENGTH LIBCAP_NAMES_BY_INDEX LIBCAP_CAP_BITS)
math(EXPR LIBCAP_CAP_NAME_SIZE "${LIBCAP_CAP_NAME_SIZE} + 1")
set(LIBCAP_CAP_NAMES)
set(libcap_cap_index 0)
foreach(capability_name IN LISTS LIBCAP_NAMES_BY_INDEX)
  string(APPEND LIBCAP_CAP_NAMES
    "      /* ${libcap_cap_index} */ \"${capability_name}\", \\\n"
  )
  math(EXPR libcap_cap_index "${libcap_cap_index} + 1")
endforeach()
file(WRITE "${LIBCAP_GENERATED_INCLUDE}/cap_names.h"
  "/* Generated from libcap's vendored UAPI capability header. */\n\n"
  "#define __CAP_BITS       ${LIBCAP_CAP_BITS}\n"
  "#define __CAP_NAME_SIZE  ${LIBCAP_CAP_NAME_SIZE}\n"
  "\n"
  "#ifdef LIBCAP_PLEASE_INCLUDE_ARRAY\n"
  "#define LIBCAP_CAP_NAMES { \\\n"
  "${LIBCAP_CAP_NAMES}"
  "  }\n"
  "#endif /* LIBCAP_PLEASE_INCLUDE_ARRAY */\n"
)

add_library(vendored_libcap STATIC
  third_party/libcap/libcap/cap_alloc.c
  third_party/libcap/libcap/cap_extint.c
  third_party/libcap/libcap/cap_file.c
  third_party/libcap/libcap/cap_flag.c
  third_party/libcap/libcap/cap_proc.c
  third_party/libcap/libcap/cap_syscalls.c
  third_party/libcap/libcap/cap_text.c
)
add_library(libcap::cap ALIAS vendored_libcap)
target_compile_definitions(vendored_libcap PRIVATE
  linux
  _LARGEFILE64_SOURCE
  _FILE_OFFSET_BITS=64
  _LIBPSX_PTHREAD_LINKAGE
)
target_include_directories(vendored_libcap
  SYSTEM PUBLIC
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/libcap/libcap/include"
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/libcap/libcap/include/uapi"
  PRIVATE
    "${LIBCAP_GENERATED_INCLUDE}"
    "${CMAKE_CURRENT_SOURCE_DIR}/third_party/libcap/libcap"
)
set_target_properties(vendored_libcap PROPERTIES
  OUTPUT_NAME cap
  C_VISIBILITY_PRESET hidden
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
  src/drivers/chain_driver.cpp
  src/drivers/chain_command_executor.cpp
  src/drivers/chain_driver_registry.cpp
  src/drivers/firo_driver.cpp
  src/http_client.cpp
  src/log_tail.cpp
  src/log_view.cpp
  src/logging.cpp
  src/main.cpp
  src/mining_difficulty.cpp
  src/network.cpp
  src/network_allocation_lock.cpp
  src/node_log_collector.cpp
  src/node_log_pane.cpp
  src/peer_count_policy.cpp
  src/peer_connectivity_controller.cpp
  src/periodic_metrics_collector.cpp
  src/process.cpp
  src/probabilistic_block_scheduler.cpp
  src/run_report.cpp
  src/simulation_command.cpp
  src/simulation_command_processor.cpp
  src/simulation_command_queue.cpp
  src/simulation_registry.cpp
  src/simulation_network_address_plan.cpp
  src/simulator_app.cpp
  src/tui.cpp
  src/tui_command_parser.cpp
  src/util.cpp
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
  libcap::cap
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
    tests/unit/firo_driver_tests.cpp
    tests/unit/log_tail_tests.cpp
    tests/unit/logging_tests.cpp
    tests/unit/network_tests.cpp
    tests/unit/simulation_network_address_plan_tests.cpp
    tests/unit/node_log_pane_tests.cpp
    tests/unit/node_runtime_tests.cpp
    tests/unit/peer_connectivity_controller_tests.cpp
    tests/unit/no_shellout_tests.cpp
    tests/unit/periodic_metrics_collector_tests.cpp
    tests/unit/process_tests.cpp
    tests/unit/probabilistic_block_scheduler_tests.cpp
    tests/unit/run_report_tests.cpp
    tests/unit/simulation_command_processor_tests.cpp
    tests/unit/simulation_command_queue_tests.cpp
    tests/unit/simulation_registry_tests.cpp
    tests/unit/tui_command_parser_tests.cpp
    tests/unit/util_tests.cpp
    src/capability.cpp
    src/cgroup.cpp
    src/block_production_policy.cpp
    src/simulation_command.cpp
    src/drivers/chain_driver.cpp
    src/drivers/firo_driver.cpp
    src/http_client.cpp
    src/log_tail.cpp
    src/log_view.cpp
    src/logging.cpp
    src/mining_difficulty.cpp
    src/network.cpp
    src/network_allocation_lock.cpp
    src/node_log_pane.cpp
    src/peer_count_policy.cpp
    src/peer_connectivity_controller.cpp
    src/periodic_metrics_collector.cpp
    src/process.cpp
    src/probabilistic_block_scheduler.cpp
    src/run_report.cpp
    src/simulation_command_processor.cpp
    src/simulation_command_queue.cpp
    src/simulation_registry.cpp
    src/simulation_network_address_plan.cpp
    src/tui_command_parser.cpp
    src/util.cpp
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
    libcap::cap
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
  add_test(NAME bbp-valid-resource-pressure-scenario
    COMMAND bbp
      --scenario-json
      "${CMAKE_CURRENT_SOURCE_DIR}/tests/scenarios/valid-resource-pressure-scenario.json"
      --probe-capabilities
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
