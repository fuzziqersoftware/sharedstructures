OBJECTS=Pool.o HashTable.o PrefixTree.o
CXX=g++
CXXFLAGS=-I/usr/local/include -std=c++14 -g -Wall -Werror
LDFLAGS=-L/usr/local/lib -std=c++14 -lstdc++ -lphosg -g

PYTHON_INCLUDES=$(shell python-config --includes)

all: cpp_only py_only

cpp_only: libsharedstructures.a cpp_test

py_only: sharedstructures.so python_test

libsharedstructures.a: $(OBJECTS)
	ar rcs libsharedstructures.a $(OBJECTS)


cpp_test: PoolTest HashTableTest PrefixTreeTest
	./PoolTest
	./PrefixTreeTest
	./HashTableTest

%Test: %Test.o $(OBJECTS)
	$(CXX) $(LDFLAGS) $^ -o $@


sharedstructures.so: $(OBJECTS) PythonModule.o
	$(CXX) $(LDFLAGS) -arch x86_64 -g -bundle -undefined dynamic_lookup -g PythonModule.o Pool.o PrefixTree.o -o sharedstructures.so

PythonModule.o: PythonModule.cc
	$(CXX) $(CXXFLAGS) -fno-strict-aliasing -fno-common -dynamic -arch i386 -arch x86_64 -g $(PYTHON_INCLUDES) -c PythonModule.cc -o PythonModule.o


python_test:
	python PrefixTreeTest.py


clean:
	rm -rf *.dSYM *.o gmon.out libsharedstructures.a *Test

.PHONY: clean python_test
