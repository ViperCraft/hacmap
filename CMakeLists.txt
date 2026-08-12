cmake_minimum_required(VERSION 3.5)

project(testing)

enable_testing()

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17 -O3 -mpopcnt -mbmi2 -DNDEBUG=1")

add_subdirectory(benchs)
add_subdirectory(test)
