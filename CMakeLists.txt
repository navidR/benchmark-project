cmake_minimum_required(VERSION 3.20)

project(benchmark_project LANGUAGES C CXX)

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
  src/capability.cpp
  src/cgroup.cpp
  src/firo_driver.cpp
  src/http_client.cpp
  src/logging.cpp
  src/main.cpp
  src/network.cpp
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
  libmnl::libmnl
)
target_compile_options(benchmark-sim PRIVATE
  -Wall
  -Wextra
  -Wpedantic
  -Wconversion
)

if(BUILD_TESTING)
  add_executable(benchmark-sim-unit-tests
    tests/unit/capability_tests.cpp
    tests/unit/cgroup_tests.cpp
    tests/unit/network_tests.cpp
    tests/unit/process_tests.cpp
    tests/unit/util_tests.cpp
    src/capability.cpp
    src/cgroup.cpp
    src/network.cpp
    src/process.cpp
    src/util.cpp
  )
  target_include_directories(benchmark-sim-unit-tests PRIVATE include)
  target_link_libraries(benchmark-sim-unit-tests PRIVATE
    Boost::json
    Boost::unit_test_framework
    libmnl::libmnl
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
