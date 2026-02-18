cmake_minimum_required(VERSION 3.16)
project(music_ir_core CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-Wall -Wextra -Wpedantic)

add_executable(tokenizer src/tokenizer.cpp)
add_executable(stemmer src/stemmer.cpp)
add_executable(term_stats src/term_stats.cpp)
add_executable(index_builder src/index_builder.cpp)
add_executable(search_cli src/search_cli.cpp)
