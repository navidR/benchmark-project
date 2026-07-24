if(NOT DEFINED TEST_COMMAND OR NOT DEFINED CAPABILITY_HELPER OR
   NOT DEFINED DAEMON_HELPER OR NOT DEFINED TEST_ROOT)
  message(FATAL_ERROR
    "TEST_COMMAND, CAPABILITY_HELPER, DAEMON_HELPER, and TEST_ROOT are required")
endif()

file(REMOVE_RECURSE "${TEST_ROOT}")
set(benchmark_root "${TEST_ROOT}/runs")
set(bin_root "${TEST_ROOT}/bin")
set(ready_daemon "${bin_root}/ready-firod")
set(firo_qt "${bin_root}/firo-qt")
file(MAKE_DIRECTORY "${benchmark_root}" "${bin_root}")
configure_file("${DAEMON_HELPER}" "${ready_daemon}" COPYONLY)
configure_file("${DAEMON_HELPER}" "${firo_qt}" COPYONLY)
file(CHMOD "${ready_daemon}" "${firo_qt}"
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE)

set(run_id "no-net-cap-cleanup")
set(run_root "${benchmark_root}/${run_id}")
set(node_root "${run_root}/nodes/firo-1")
execute_process(
  COMMAND "${TEST_COMMAND}"
    --benchmark-root "${benchmark_root}"
    --run-id "${run_id}"
    --nodes 1
    --node-binary "${ready_daemon}"
    --no-isolate-network
    --metrics-sample-count 1
    --metrics-interval 50ms
    --keep-cgroups
    --no-tui
  RESULT_VARIABLE prepare_result
  OUTPUT_VARIABLE prepare_stdout
  ERROR_VARIABLE prepare_stderr
)
if(NOT prepare_result EQUAL 0)
  message(FATAL_ERROR
    "could not prepare non-isolated retained run: ${prepare_stdout}${prepare_stderr}")
endif()
file(READ "${run_root}/.bbp-run" ownership)
string(JSON resource_id GET "${ownership}" resource_id)
set(cgroup_root "/sys/fs/cgroup/bbp/${resource_id}")
file(WRITE "${node_root}/.bbp-rpc-cookie" "owned-secret\n")
file(WRITE "${run_root}/sentinel" "run directory survives cleanup\n")

execute_process(
  COMMAND "${CAPABILITY_HELPER}" "${TEST_COMMAND}"
    --benchmark-root "${benchmark_root}"
    --cleanup-run "${run_id}"
  RESULT_VARIABLE result
  OUTPUT_VARIABLE stdout
  ERROR_VARIABLE stderr
)
set(output "${stdout}${stderr}")
if(NOT result EQUAL 0)
  execute_process(
    COMMAND "${TEST_COMMAND}"
      --benchmark-root "${benchmark_root}"
      --cleanup-run "${run_id}"
    OUTPUT_QUIET ERROR_QUIET
  )
  message(FATAL_ERROR
    "non-isolated cleanup failed without CAP_NET_ADMIN: ${output}")
endif()
if(NOT output MATCHES "effective_cap_net_admin=0")
  message(FATAL_ERROR "capability helper did not prove CAP_NET_ADMIN absence")
endif()
if(EXISTS "${cgroup_root}")
  message(FATAL_ERROR "non-isolated cleanup retained its owned cgroup")
endif()
if(EXISTS "${node_root}/.bbp-rpc-cookie")
  message(FATAL_ERROR "non-isolated cleanup retained its RPC credential")
endif()
if(NOT EXISTS "${run_root}/sentinel")
  message(FATAL_ERROR "non-isolated cleanup crossed its resource boundary")
endif()

set(isolated_run_id "isolated-net-cleanup")
set(isolated_run_root "${benchmark_root}/${isolated_run_id}")
set(isolated_node_root "${isolated_run_root}/nodes/firo-1")
execute_process(
  COMMAND "${TEST_COMMAND}"
    --benchmark-root "${benchmark_root}"
    --run-id "${isolated_run_id}"
    --nodes 1
    --node-binary "${ready_daemon}"
    --isolate-network
    --metrics-sample-count 1
    --metrics-interval 50ms
    --keep-cgroups
    --no-tui
  RESULT_VARIABLE isolated_prepare_result
  OUTPUT_VARIABLE isolated_prepare_stdout
  ERROR_VARIABLE isolated_prepare_stderr
)
if(NOT isolated_prepare_result EQUAL 0)
  message(FATAL_ERROR
    "could not prepare isolated retained run: ${isolated_prepare_stdout}${isolated_prepare_stderr}")
endif()
file(READ "${isolated_run_root}/.bbp-run" isolated_ownership)
string(JSON isolated_resource_id GET "${isolated_ownership}" resource_id)
set(isolated_cgroup_root
  "/sys/fs/cgroup/bbp/${isolated_resource_id}")
file(WRITE "${isolated_node_root}/.bbp-rpc-cookie" "owned-secret\n")
execute_process(
  COMMAND "${CAPABILITY_HELPER}" "${TEST_COMMAND}"
    --benchmark-root "${benchmark_root}"
    --cleanup-run "${isolated_run_id}"
  RESULT_VARIABLE isolated_result
  OUTPUT_VARIABLE isolated_stdout
  ERROR_VARIABLE isolated_stderr
)
set(isolated_output "${isolated_stdout}${isolated_stderr}")
if(isolated_result EQUAL 0 OR
   NOT isolated_output MATCHES "missing required capability: CAP_NET_ADMIN")
  message(FATAL_ERROR
    "isolated cleanup did not retain its network capability gate: ${isolated_output}")
endif()
if(NOT EXISTS "${isolated_cgroup_root}" OR
   NOT EXISTS "${isolated_node_root}/.bbp-rpc-cookie")
  message(FATAL_ERROR
    "isolated cleanup mutated resources before its capability gate")
endif()
execute_process(
  COMMAND "${TEST_COMMAND}"
    --benchmark-root "${benchmark_root}"
    --cleanup-run "${isolated_run_id}"
  RESULT_VARIABLE isolated_cleanup_result
  OUTPUT_VARIABLE isolated_cleanup_stdout
  ERROR_VARIABLE isolated_cleanup_stderr
)
if(NOT isolated_cleanup_result EQUAL 0)
  message(FATAL_ERROR
    "privileged isolated cleanup failed: ${isolated_cleanup_stdout}${isolated_cleanup_stderr}")
endif()

file(REMOVE_RECURSE "${TEST_ROOT}")
