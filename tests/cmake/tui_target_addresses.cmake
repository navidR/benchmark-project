if(NOT DEFINED TEST_COMMAND OR NOT DEFINED FIXTURE OR NOT DEFINED TEST_ROOT)
  message(FATAL_ERROR "TEST_COMMAND, FIXTURE, and TEST_ROOT are required")
endif()

file(MAKE_DIRECTORY "${TEST_ROOT}")
file(COPY "${FIXTURE}/" DESTINATION "${TEST_ROOT}")
file(APPEND "${TEST_ROOT}/events.jsonl" [=[
{"run_id":"tui-fixture","node_id":"sim","event":"operator_command_completed","detail":"{\"kind\":\"add_target_address\",\"target_address\":\"target-visible-address\"}"}
{"run_id":"tui-fixture","node_id":"firo-1","event":"wallet_transaction_submitted","detail":"{\"sender_wallet_index\":1,\"receiver_wallet_index\":null,\"receiver_node\":null,\"receiver_address\":\"target-visible-address\",\"external_receiver\":true,\"amount_satoshis\":1000}"}
]=])
string(ASCII 27 escape)
file(WRITE "${TEST_ROOT}/input" "wc${escape}qy")
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env TERM=xterm COLUMNS=160 LINES=64
    "${TEST_COMMAND}" --run "${TEST_ROOT}"
  INPUT_FILE "${TEST_ROOT}/input"
  RESULT_VARIABLE result
  OUTPUT_VARIABLE stdout
  ERROR_VARIABLE stderr
  TIMEOUT 10
)
set(output "${stdout}${stderr}")
if(NOT result EQUAL 0)
  message(FATAL_ERROR "target-address TUI sequence failed (${result}):\n${output}")
endif()
foreach(expected "External Targets" "target-visible-address" "submitted=1"
                 "add-target <address>" "remove-target <address>"
                 "resource-profile <name>")
  string(FIND "${output}" "${expected}" found)
  if(found EQUAL -1)
    message(FATAL_ERROR "target-address TUI missing '${expected}':\n${output}")
  endif()
endforeach()
