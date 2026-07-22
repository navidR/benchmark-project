if(NOT DEFINED TEST_COMMAND)
  message(FATAL_ERROR "TEST_COMMAND is required")
endif()
if(NOT DEFINED SCENARIO)
  message(FATAL_ERROR "SCENARIO is required")
endif()

function(write_scenario last_index)
  set(actions "")
  foreach(index RANGE 0 ${last_index})
    if(NOT actions STREQUAL "")
      string(APPEND actions ",")
    endif()
    string(APPEND actions
      "{\"type\":\"checkpoint\",\"name\":\"c${index}\"}")
  endforeach()
  file(WRITE "${SCENARIO}"
    "{\"chains\":{\"firo\":{\"driver\":\"firo\",\"default_binary\":\"/tmp/not-used-by-probe\"}},\"chain\":\"firo\",\"nodes\":1,\"workloads\":[${actions}]}\n")
endfunction()

write_scenario(255)
execute_process(
  COMMAND "${TEST_COMMAND}" --scenario-json "${SCENARIO}"
          --probe-capabilities
  RESULT_VARIABLE accepted_result
  OUTPUT_VARIABLE accepted_output
  ERROR_VARIABLE accepted_error
)
if(NOT accepted_result EQUAL 0)
  message(FATAL_ERROR
    "scenario at retained limit failed:\n${accepted_output}${accepted_error}")
endif()

write_scenario(256)

execute_process(
  COMMAND "${TEST_COMMAND}" --scenario-json "${SCENARIO}"
  RESULT_VARIABLE result
  OUTPUT_VARIABLE output
  ERROR_VARIABLE error
)

set(combined_output "${output}${error}")
if(result EQUAL 0)
  message(FATAL_ERROR "oversized scenario unexpectedly succeeded")
endif()
if(NOT combined_output MATCHES
   "scenario action count exceeds retained limit 256")
  message(FATAL_ERROR "unexpected command output:\n${combined_output}")
endif()
