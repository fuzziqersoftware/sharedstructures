OBJECTS=Pool.o HashTable.o PrefixTree.o
CXX=g++ -fPIC
CXXFLAGS=-I/usr/local/include -std=c++14 -g -Wall -Werror
LDFLAGS=-L/usr/local/lib -std=c++14 -lphosg -g

INSTALL_DIR=/usr/local

PYTHON_INCLUDES=$(shell python-config --includes)


UNAME = $(shell uname -s)
ifeq ($(UNAME),Darwin)
	CXXFLAGS +=  -arch i386 -arch x86_64 -DMACOSX
	LDFLAGS +=  -arch i386 -arch x86_64
	PYTHON_MODULE_CXXFLAGS = -dynamic -DMACOSX
	PYTHON_MODULE_LDFLAGS = -bundle -undefined dynamic_lookup
endif
ifeq ($(UNAME),Linux)
	CXXFLAGS +=  -DLINUX
	LDFLAGS +=  -lrt -pthread
	PYTHON_MODULE_CXXFLAGS = -DLINUX
	PYTHON_MODULE_LDFLAGS = -shared
endif

all: cpp_only py_only

install: libsharedstructures.a
	mkdir -p $(INSTALL_DIR)/include/sharedstructures
	cp libsharedstructures.a $(INSTALL_DIR)/lib/
	cp -r *.hh $(INSTALL_DIR)/include/sharedstructures/

cpp_only: libsharedstructures.a cpp_test

py_only: sharedstructures.so py_test

libsharedstructures.a: $(OBJECTS)
	rm -f libsharedstructures.a
	ar rcs libsharedstructures.a $(OBJECTS)


cpp_test: PoolTest HashTableTest PrefixTreeTest AllocatorBenchmark
	./PoolTest
	./PrefixTreeTest
	./HashTableTest

osx_cpp32_test: cpp_test
	arch -32 ./PoolTest
	arch -32 ./PrefixTreeTest
	arch -32 ./HashTableTest

%Test: %Test.o $(OBJECTS)
	$(CXX) $^ $(LDFLAGS) -o $@

AllocatorBenchmark: AllocatorBenchmark.o $(OBJECTS)
	$(CXX) $^ $(LDFLAGS) -o $@


sharedstructures.so: $(OBJECTS) PythonModule.o
	$(CXX) $^ $(PYTHON_MODULE_LDFLAGS) $(LDFLAGS) -o $@

PythonModule.o: PythonModule.cc
	$(CXX) $(CXXFLAGS) $(PYTHON_MODULE_CXXFLAGS) -fno-strict-aliasing -fno-common -g $(PYTHON_INCLUDES) -c PythonModule.cc -o PythonModule.o


py_test:
	python HashTableTest.py
	python PrefixTreeTest.py

osx_py32_test: py_test
	arch -32 python HashTableTest.py
	arch -32 python PrefixTreeTest.py


clean:
	rm -rf *.dSYM *.o gmon.out libsharedstructures.a sharedstructures.so *Test

.PHONY: all cpp_only py_only clean cpp_test py_test osx_cpp32_test osx_py32_test
