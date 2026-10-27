CXX      ?= clang++
CXXFLAGS ?= -std=c++17 -O3 -march=native -Wall -Wextra

colquery: src/main.cpp src/*.h
	$(CXX) $(CXXFLAGS) -o $@ src/main.cpp

test: colquery
	./tests/run_tests.sh

clean:
	rm -f colquery
	rm -rf build_data

.PHONY: test clean
