if(NOT DEFINED TEST_COMMAND)
  message(FATAL_ERROR "TEST_COMMAND is required")
endif()
if(NOT DEFINED ARGS)
  message(FATAL_ERROR "ARGS is required")
endif()
if(NOT DEFINED REGEX)
  message(FATAL_ERROR "REGEX is required")
endif()

string(REPLACE "|" ";" command_arguments "${ARGS}")
execute_process(
  COMMAND "${TEST_COMMAND}" ${command_arguments}
  RESULT_VARIABLE result
  OUTPUT_VARIABLE output
  ERROR_VARIABLE error
)

set(combined_output "${output}${error}")
if(result EQUAL 0)
  message(FATAL_ERROR "command unexpectedly succeeded: ${TEST_COMMAND}")
endif()
if(NOT combined_output MATCHES "${REGEX}")
  message(FATAL_ERROR
    "command output did not match '${REGEX}':\n${combined_output}"
  )
endif()
