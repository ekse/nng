cmake_minimum_required (VERSION 2.8.7)

project(raw)

set(PARALLEL 128 CACHE STRING "Parallelism (min 4, max 1000)")

find_package(nng CONFIG REQUIRED)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fsanitize=address")

add_executable(irp irp.c)
target_link_libraries(irp nng::nng)
target_compile_definitions(irp PRIVATE -DPARALLEL=${PARALLEL})
