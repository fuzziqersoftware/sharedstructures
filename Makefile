OBJECTS=Pool.o HashTable.o PrefixTree.o
CXX=g++
CXXFLAGS=-I/usr/local/include -std=c++14 -g -Wall -Werror -arch i386 -arch x86_64
LDFLAGS=-L/usr/local/lib -std=c++14 -lstdc++ -lphosg -g -arch i386 -arch x86_64

PYTHON_INCLUDES=$(shell python-config --includes)

all: cpp_only py_only

cpp_only: libsharedstructures.a cpp_test

py_only: sharedstructures.so py_test

libsharedstructures.a: $(OBJECTS)
	ar rcs libsharedstructures.a $(OBJECTS)


cpp_test: PoolTest HashTableTest PrefixTreeTest
	./PoolTest
	./PrefixTreeTest
	./HashTableTest

osx_cpp32_test: cpp_test
	arch -32 ./PoolTest
	arch -32 ./PrefixTreeTest
	arch -32 ./HashTableTest

%Test: %Test.o $(OBJECTS)
	$(CXX) $(LDFLAGS) $^ -o $@


sharedstructures.so: $(OBJECTS) PythonModule.o
	$(CXX) $(LDFLAGS) -g -bundle -undefined dynamic_lookup -g PythonModule.o Pool.o PrefixTree.o -o sharedstructures.so

PythonModule.o: PythonModule.cc
	$(CXX) $(CXXFLAGS) -fno-strict-aliasing -fno-common -dynamic -g $(PYTHON_INCLUDES) -c PythonModule.cc -o PythonModule.o


py_test:
	python PrefixTreeTest.py

osx_py32_test: py_test
	arch -32 python PrefixTreeTest.py


clean:
	rm -rf *.dSYM *.o gmon.out libsharedstructures.a *Test

.PHONY: all cpp_only py_only clean cpp_test py_test osx_cpp32_test osx_py32_test
