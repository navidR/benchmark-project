if(NOT DEFINED TEST_COMMAND OR NOT DEFINED TEST_ROOT)
  message(FATAL_ERROR "TEST_COMMAND and TEST_ROOT are required")
endif()

set(run_id "copied-owner")
set(first_root "${TEST_ROOT}/first")
set(second_root "${TEST_ROOT}/second")
set(first_run "${first_root}/${run_id}")
set(second_run "${second_root}/${run_id}")
file(REMOVE_RECURSE "${TEST_ROOT}")
file(MAKE_DIRECTORY "${first_run}" "${second_run}")
file(WRITE "${first_run}/.bbp-run"
  "{\"version\":1,\"run_id\":\"${run_id}\",\"run_root\":\"${first_run}\",\"resource_id\":\"0123456789abcdef0123456789abcdef\"}\n"
)
file(COPY_FILE "${first_run}/.bbp-run" "${second_run}/.bbp-run")
file(WRITE "${second_run}/foreign-sentinel" "must survive\n")

execute_process(
  COMMAND "${TEST_COMMAND}"
    --benchmark-root "${second_root}"
    --cleanup-run "${run_id}"
  RESULT_VARIABLE result
  OUTPUT_VARIABLE stdout
  ERROR_VARIABLE stderr
)

if(result EQUAL 0)
  message(FATAL_ERROR "cleanup unexpectedly accepted a copied run marker")
endif()
set(output "${stdout}${stderr}")
if(NOT output MATCHES "run ownership marker root does not match")
  message(FATAL_ERROR "cleanup failed for the wrong reason: ${output}")
endif()
if(NOT EXISTS "${second_run}/foreign-sentinel")
  message(FATAL_ERROR "cleanup removed a foreign file after marker rejection")
endif()

file(REMOVE_RECURSE "${TEST_ROOT}")
