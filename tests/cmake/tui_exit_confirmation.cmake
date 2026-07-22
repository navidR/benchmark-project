if(NOT DEFINED TEST_COMMAND OR NOT DEFINED RUN_ROOT OR
   NOT DEFINED TEST_INPUT)
  message(FATAL_ERROR "TEST_COMMAND, RUN_ROOT, and TEST_INPUT are required")
endif()

set(events_path "${RUN_ROOT}/events.jsonl")
set(scenario_path "${RUN_ROOT}/resolved-scenario.json")
file(SHA256 "${events_path}" events_before)
file(SHA256 "${scenario_path}" scenario_before)

string(ASCII 27 escape)
file(WRITE "${TEST_INPUT}" "${escape}n${escape}y")
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env TERM=xterm COLUMNS=160 LINES=40
    "${TEST_COMMAND}" --run "${RUN_ROOT}"
  INPUT_FILE "${TEST_INPUT}"
  RESULT_VARIABLE result
  OUTPUT_VARIABLE stdout
  ERROR_VARIABLE stderr
  TIMEOUT 10
)
file(REMOVE "${TEST_INPUT}")

set(output "${stdout}${stderr}")
if(NOT result EQUAL 0)
  message(FATAL_ERROR
    "TUI exit confirmation sequence failed (${result}):\n${output}")
endif()
string(REGEX MATCHALL "Confirm exit" prompts "${output}")
list(LENGTH prompts prompt_count)
if(NOT prompt_count EQUAL 2)
  message(FATAL_ERROR
    "expected two rendered exit confirmations, found ${prompt_count}:\n${output}")
endif()
if(NOT output MATCHES "Press y to exit; n or Esc cancels\\.")
  message(FATAL_ERROR "exit confirmation instructions were not rendered")
endif()
if(NOT output MATCHES "status: incomplete")
  message(FATAL_ERROR "active main window was not rendered")
endif()

file(SHA256 "${events_path}" events_after)
file(SHA256 "${scenario_path}" scenario_after)
if(NOT events_before STREQUAL events_after OR
   NOT scenario_before STREQUAL scenario_after)
  message(FATAL_ERROR "exit cancellation changed active run artifacts")
endif()
