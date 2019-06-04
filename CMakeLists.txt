cmake_minimum_required(VERSION 3.2)
project(unats CXX)

set(CMAKE_CXX_STANDARD 17)

add_library(unats INTERFACE)
target_include_directories(unats INTERFACE include/)

add_executable(subscriber examples/subscriber.cpp)
target_link_libraries(subscriber unats)
target_compile_options(subscriber PRIVATE -Wall -Wextra -Wpedantic -Wno-sign-compare -Wno-pointer-arith -Wno-unused-parameter)

add_executable(publisher examples/publisher.cpp)
target_link_libraries(publisher unats)
target_compile_options(publisher PRIVATE -Wall -Wextra -Wpedantic -Wno-sign-compare -Wno-pointer-arith -Wno-unused-parameter)