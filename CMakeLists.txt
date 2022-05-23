cmake_minimum_required(VERSION 3.18)



# Project setup

project(sharedstructures)

set(CMAKE_BUILD_TYPE Debug)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED True)
if (MSVC)
  add_compile_options(/W4 /WX)
else()
  add_compile_options(-Wall -Wextra -Werror -Wno-cast-function-type -Wno-missing-field-initializers)
endif()

set(CMAKE_POSITION_INDEPENDENT_CODE ON)

include_directories("/usr/local/include")
include_directories("/opt/homebrew/include")
link_directories("/usr/local/lib")
link_directories("/opt/homebrew/lib")

if(UNIX AND NOT APPLE)
  set(LINUX TRUE)
endif()



# Library dependencies

find_package(Python3 COMPONENTS Interpreter Development.Module)

if(${Python3_FOUND})
	message(STATUS "Building Python module")
else()
	message(WARNING "Python not found; not building Python module")
endif()



get_cmake_property(_variableNames VARIABLES)
list (SORT _variableNames)
foreach (_variableName ${_variableNames})
    message(STATUS "${_variableName}=${${_variableName}}")
endforeach()



# Library definitions (used for both bundled and non-bundled executables)

add_library(sharedstructures
  src/Allocator.cc
  src/HashTable.cc
  src/IntVector.cc
  src/LogarithmicAllocator.cc
  src/Pool.cc
  src/PrefixTree.cc
  src/PriorityQueue.cc
  src/ProcessLock.cc
  src/Queue.cc
  src/SimpleAllocator.cc
)
target_include_directories(sharedstructures PUBLIC ${Python3_INCLUDE_DIR})
target_link_libraries(sharedstructures PUBLIC phosg ${Python3_LIBRARY_DIRS})
if (LINUX)
	target_link_libraries(sharedstructures PUBLIC rt)
endif()

if(${Python3_FOUND})
	add_library(sharedstructures.abi3 SHARED src/PythonModule.cc)
	target_include_directories(sharedstructures.abi3 PUBLIC ${Python3_INCLUDE_DIRS})
  target_link_libraries(sharedstructures.abi3 sharedstructures)
  set_target_properties(sharedstructures.abi3 PROPERTIES
    PREFIX ""
    OUTPUT_NAME "sharedstructures.abi3"
    LINKER_LANGUAGE C
  )
  # On macOS, the linker will complain about undefined symbols in the Python
	# module (which is expected because those symbols are defined in the Python
	# interpreter executable, not in a library). Also, Python expects a .so
	# suffix, even on macOS (where .dylib is normally used instead).
	if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "AppleClang")
	  target_link_options(sharedstructures.abi3 PUBLIC -undefined dynamic_lookup)
	  set_target_properties(sharedstructures.abi3 PROPERTIES SUFFIX ".so")
	endif()
endif()



# Test and benchmark executables

enable_testing()

foreach(TestName IN ITEMS AllocatorTest HashTableTest IntVectorTest PrefixTreeTest PriorityQueueTest ProcessLockTest QueueTest)
	add_executable(${TestName} src/${TestName}.cc)
	target_link_libraries(${TestName} sharedstructures)
	add_test(NAME ${TestName} COMMAND ${TestName})
endforeach()

if(${Python3_FOUND})
  foreach(PyTestName IN ITEMS HashTableTest IntVectorTest PrefixTreeTest PriorityQueueTest QueueTest)
	  add_test(NAME Python3_${PyTestName} COMMAND python3 src/${PyTestName}.py)
	  set_property(TEST Python3_${PyTestName} PROPERTY WORKING_DIRECTORY ${CMAKE_BINARY_DIR})
	  set_property(TEST Python3_${PyTestName} PROPERTY ENVIRONMENT "PYTHONPATH=.")
  endforeach()
endif()

foreach(BenchmarkName IN ITEMS AllocatorBenchmark PrefixTreeBenchmark)
	add_executable(${BenchmarkName} src/${BenchmarkName}.cc)
	target_link_libraries(${BenchmarkName} sharedstructures)
endforeach()



# Installation configuration

file(GLOB Headers ${CMAKE_SOURCE_DIR}/src/*.hh)
install(TARGETS sharedstructures DESTINATION lib)
install(FILES ${Headers} DESTINATION include/sharedstructures)



# macOS
# 	PYTHON_MODULE_CXXFLAGS = -dynamic -DMACOSX
# 	PYTHON_MODULE_LDFLAGS = -bundle -undefined dynamic_lookup
# Linux
# 	PYTHON_MODULE_CXXFLAGS = -DLINUX
# 	PYTHON_MODULE_LDFLAGS = -shared
# 
# sharedstructures.abi3.so: $(OBJECTS) PythonModule.o
# 	$(CXX) $^ $(PYTHON_MODULE_LDFLAGS) $(LDFLAGS) -o $@
# 
# PythonModule.o: PythonModule.cc
# 	$(CXX) $(CXXFLAGS) $(PYTHON_MODULE_CXXFLAGS) -fno-strict-aliasing -fno-common -g $(PYTHON_INCLUDES) -c PythonModule.cc -o PythonModule.o
