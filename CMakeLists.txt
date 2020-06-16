cmake_minimum_required(VERSION 3.14)

project(MiniBusCpp)

set(CMAKE_CXX_STANDARD 17)

find_package(Threads REQUIRED)
find_package(Boost REQUIRED COMPONENTS system)

add_library(mini_bus INTERFACE)
target_include_directories(mini_bus INTERFACE include/)
target_link_libraries(mini_bus INTERFACE Boost::system Threads::Threads)

install(
    DIRECTORY ${CMAKE_SOURCE_DIR}/include/
    DESTINATION include
    FILES_MATCHING PATTERN "*.hpp")

add_executable(demo "src/demo.cpp")
target_link_libraries(demo PRIVATE mini_bus)

add_executable(demo_server "src/demo_server.cpp")
target_link_libraries(demo_server PRIVATE mini_bus)