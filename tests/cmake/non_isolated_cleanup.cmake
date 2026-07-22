if(NOT DEFINED TEST_COMMAND OR NOT DEFINED CAPABILITY_HELPER OR
   NOT DEFINED TEST_ROOT)
  message(FATAL_ERROR
    "TEST_COMMAND, CAPABILITY_HELPER, and TEST_ROOT are required")
endif()

set(run_id "no-net-cap-cleanup")
string(SHA256 resource_hash "${TEST_ROOT}")
string(SUBSTRING "${resource_hash}" 0 32 resource_id)
set(run_root "${TEST_ROOT}/${run_id}")
set(node_root "${run_root}/nodes/firo-1")
set(cgroup_root "/sys/fs/cgroup/bbp/${resource_id}")

file(REMOVE_RECURSE "${TEST_ROOT}")
file(MAKE_DIRECTORY "${node_root}")
file(WRITE "${run_root}/.bbp-run"
  "{\"version\":1,\"run_id\":\"${run_id}\",\"run_root\":\"${run_root}\",\"resource_id\":\"${resource_id}\"}\n"
)
file(WRITE "${run_root}/resolved-scenario.json"
  "{\"chain\":\"firo\",\"nodes\":1,\"isolated_network\":false}\n"
)
file(WRITE "${node_root}/.bbp-rpc-cookie" "owned-secret\n")
file(WRITE "${run_root}/sentinel" "run directory survives cleanup\n")
file(MAKE_DIRECTORY "${cgroup_root}")

execute_process(
  COMMAND "${CAPABILITY_HELPER}" "${TEST_COMMAND}"
    --benchmark-root "${TEST_ROOT}"
    --cleanup-run "${run_id}"
  RESULT_VARIABLE result
  OUTPUT_VARIABLE stdout
  ERROR_VARIABLE stderr
)
set(output "${stdout}${stderr}")
if(NOT result EQUAL 0)
  execute_process(
    COMMAND "${TEST_COMMAND}"
      --benchmark-root "${TEST_ROOT}"
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
string(SHA256 isolated_resource_hash "${TEST_ROOT}-isolated")
string(SUBSTRING "${isolated_resource_hash}" 0 32 isolated_resource_id)
set(isolated_run_root "${TEST_ROOT}/${isolated_run_id}")
set(isolated_node_root "${isolated_run_root}/nodes/firo-1")
set(isolated_cgroup_root
  "/sys/fs/cgroup/bbp/${isolated_resource_id}")
file(MAKE_DIRECTORY "${isolated_node_root}" "${isolated_cgroup_root}")
file(WRITE "${isolated_run_root}/.bbp-run"
  "{\"version\":1,\"run_id\":\"${isolated_run_id}\",\"run_root\":\"${isolated_run_root}\",\"resource_id\":\"${isolated_resource_id}\"}\n"
)
file(WRITE "${isolated_run_root}/resolved-scenario.json"
  "{\"chain\":\"firo\",\"nodes\":1,\"isolated_network\":true}\n"
)
file(WRITE "${isolated_node_root}/.bbp-rpc-cookie" "owned-secret\n")
execute_process(
  COMMAND "${CAPABILITY_HELPER}" "${TEST_COMMAND}"
    --benchmark-root "${TEST_ROOT}"
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
    --benchmark-root "${TEST_ROOT}"
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
