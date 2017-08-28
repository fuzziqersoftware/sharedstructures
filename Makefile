OBJECTS=Pool.o ProcessLock.o Allocator.o SimpleAllocator.o LogarithmicAllocator.o HashTable.o PrefixTree.o
CXX=g++ -fPIC
CXXFLAGS=-std=c++14 -g -Wall -Werror
LDFLAGS=-std=c++14 -lphosg -g

INSTALL_DIR=/usr/local

PYTHON_INCLUDES=$(shell python-config --includes)
PYTHON3_INCLUDES=$(shell python3-config --includes)
PYTHON_LIBS=$(shell python-config --libs)
PYTHON3_LIBS=$(shell python3-config --libs)


UNAME = $(shell uname -s)
ifeq ($(UNAME),Darwin)
	CXXFLAGS += -DMACOSX -I/opt/local/include
	LDFLAGS += -L/opt/local/lib
	PYTHON_MODULE_CXXFLAGS = -dynamic -DMACOSX
	PYTHON_MODULE_LDFLAGS = -bundle -undefined dynamic_lookup
endif
ifeq ($(UNAME),Linux)
	CXXFLAGS += -DLINUX -I/usr/local/include
	LDFLAGS += -lrt -pthread -L/usr/local/lib
	PYTHON_MODULE_CXXFLAGS = -DLINUX
	PYTHON_MODULE_LDFLAGS = -shared
endif

all: cpp_only py_only py3_only

test: cpp_test py_test py3_test

install: libsharedstructures.a
	mkdir -p $(INSTALL_DIR)/include/sharedstructures
	cp libsharedstructures.a $(INSTALL_DIR)/lib/
	cp -r *.hh $(INSTALL_DIR)/include/sharedstructures/

cpp_only: libsharedstructures.a

py_only: sharedstructures.so

py3_only: sharedstructures.abi3.so

libsharedstructures.a: $(OBJECTS)
	rm -f libsharedstructures.a
	ar rcs libsharedstructures.a $(OBJECTS)


cpp_test: AllocatorTest HashTableTest PrefixTreeTest ProcessLockTest AllocatorBenchmark PrefixTreeBenchmark
	./ProcessLockTest
	./AllocatorTest
	./PrefixTreeTest
	./HashTableTest

%Test: %Test.o $(OBJECTS)
	$(CXX) $^ $(LDFLAGS) -o $@

AllocatorBenchmark: AllocatorBenchmark.o $(OBJECTS)
	$(CXX) $^ $(LDFLAGS) -o $@

PrefixTreeBenchmark: PrefixTreeBenchmark.o $(OBJECTS)
	$(CXX) $^ $(LDFLAGS) -o $@


sharedstructures.so: $(OBJECTS) PythonModule.o
	$(CXX) $^ $(PYTHON_MODULE_LDFLAGS) $(LDFLAGS) -o $@

sharedstructures.abi3.so: $(OBJECTS) PythonModule3.o
	$(CXX) $^ $(PYTHON_MODULE_LDFLAGS) $(LDFLAGS) -o $@

PythonModule.o: PythonModule.cc
	$(CXX) $(CXXFLAGS) $(PYTHON_MODULE_CXXFLAGS) -fno-strict-aliasing -fno-common -g $(PYTHON_INCLUDES) -c PythonModule.cc -o PythonModule.o

PythonModule3.o: PythonModule.cc
	$(CXX) $(CXXFLAGS) $(PYTHON_MODULE_CXXFLAGS) -fno-strict-aliasing -fno-common -g $(PYTHON3_INCLUDES) -c PythonModule.cc -o PythonModule3.o


py_test: sharedstructures.so
	python HashTableTest.py
	python PrefixTreeTest.py

py3_test: sharedstructures.abi3.so
	python3 HashTableTest.py
	python3 PrefixTreeTest.py


clean:
	rm -rf *.dSYM *.o gmon.out libsharedstructures.a sharedstructures.so sharedstructures.abi3.so *Test AllocatorBenchmark PrefixTreeBenchmark

.PHONY: all cpp_only py_only clean cpp_test py_test py3_test
