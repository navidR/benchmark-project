foreach(required_variable IN ITEMS COMPILE_COMMANDS SOURCE_DIR)
  if(NOT DEFINED ${required_variable})
    message(FATAL_ERROR "${required_variable} is required")
  endif()
endforeach()

file(READ "${COMPILE_COMMANDS}" compile_commands_json)
string(JSON compile_command_count LENGTH "${compile_commands_json}")
math(EXPR last_compile_command "${compile_command_count} - 1")

set(missing_commands
  bbp:firo_driver.cpp
  bbp:perf_counter.cpp
  bbp-unit-tests:firo_driver.cpp
  bbp-unit-tests:perf_counter.cpp
)
set(vendored_include
  "${SOURCE_DIR}/third_party/boost/libs/multiprecision/include"
)

foreach(index RANGE ${last_compile_command})
  string(JSON source_file GET "${compile_commands_json}" ${index} file)
  get_filename_component(source_name "${source_file}" NAME)
  if(NOT source_name STREQUAL "firo_driver.cpp" AND
     NOT source_name STREQUAL "perf_counter.cpp")
    continue()
  endif()

  string(JSON output_file GET "${compile_commands_json}" ${index} output)
  if(output_file MATCHES "^CMakeFiles/bbp\\.dir/")
    set(target_name bbp)
  elseif(output_file MATCHES "^CMakeFiles/bbp-unit-tests\\.dir/")
    set(target_name bbp-unit-tests)
  else()
    continue()
  endif()

  string(JSON compile_command GET "${compile_commands_json}" ${index} command)
  string(FIND "${compile_command}" "${vendored_include}" include_position)
  if(include_position EQUAL -1)
    message(FATAL_ERROR
      "${target_name}'s ${source_name} compile command does not use "
      "the vendored Boost.Multiprecision include directory"
    )
  endif()

  list(REMOVE_ITEM missing_commands "${target_name}:${source_name}")
endforeach()

if(missing_commands)
  list(JOIN missing_commands ", " missing_commands_text)
  message(FATAL_ERROR
    "compile_commands.json is missing expected entries: ${missing_commands_text}"
  )
endif()
