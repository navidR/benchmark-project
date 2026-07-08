cmake_minimum_required(VERSION 3.20)

project(benchmark_project LANGUAGES CXX)

include(CTest)

set(BOOST_INCLUDE_LIBRARIES
  asio
  beast
  json
  log
  program_options
  system
  test
)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Boost.Regex can discover optional ICU support; the simulator does not vendor
# ICU, so keep the vendored Boost build from creating system-ICU targets.
set(CMAKE_DISABLE_FIND_PACKAGE_ICU ON)
add_subdirectory(third_party/boost EXCLUDE_FROM_ALL)

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

add_executable(benchmark-sim
  src/cgroup.cpp
  src/firo_driver.cpp
  src/http_client.cpp
  src/logging.cpp
  src/main.cpp
  src/process.cpp
  src/util.cpp
)

target_include_directories(benchmark-sim PRIVATE include)
target_link_libraries(benchmark-sim PRIVATE
  Boost::asio
  Boost::beast
  Boost::json
  Boost::log
  Boost::log_setup
  Boost::program_options
  Boost::system
)
target_compile_options(benchmark-sim PRIVATE
  -Wall
  -Wextra
  -Wpedantic
  -Wconversion
)

if(BUILD_TESTING)
  add_executable(benchmark-sim-unit-tests
    tests/unit/util_tests.cpp
    src/util.cpp
  )
  target_include_directories(benchmark-sim-unit-tests PRIVATE include)
  target_link_libraries(benchmark-sim-unit-tests PRIVATE
    Boost::json
    Boost::unit_test_framework
  )
  target_compile_options(benchmark-sim-unit-tests PRIVATE
    -Wall
    -Wextra
    -Wpedantic
    -Wconversion
  )
  add_test(NAME benchmark-sim-unit-tests COMMAND benchmark-sim-unit-tests)
endif()

install(TARGETS benchmark-sim RUNTIME DESTINATION bin)
