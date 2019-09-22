CXX = g++
CC = $(CXX)
CXXFLAGS = -O3 -march=native -std=c++17 -Wall -Wextra -pedantic
LDLIBS = -ldl -lpthread -lstdc++fs -lpq

COMPONENTS = clouds clouds/gcp common infrastructure output sim

HEADERS := $(wildcard $(COMPONENTS:%=gacspp/%/*.h*))
SOURCES := $(wildcard $(COMPONENTS:%=gacspp/%/*.cpp))
OBJFILES := $(SOURCES:%.cpp=%.o)
OBJFILES := $(OBJFILES:gacspp/%=bin/%)

gacspp.out: $(OBJFILES)
	$(CXX) -o gacspp.out $(OBJFILES) $(LDLIBS)

$(OBJFILES): bin/%.o: gacspp/%.cpp $(HEADERS)
	mkdir -p $(@D)
	$(CXX) -c $(CXXFLAGS) -Igacspp/ $< -o $@