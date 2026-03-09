# ==========================================
# ForgeDB Makefile
# ==========================================
CXX      = clang++
CXXFLAGS = -Wall -Wextra -std=c++17 -Iinclude -MMD -MP

# Source files
SRCS = src/main.cpp src/pager.cpp src/node.cpp src/btree.cpp src/bloom.cpp src/utils.cpp src/tokenizer.cpp src/parser.cpp
OBJS = $(SRCS:.cpp=.o)
DEPS = $(OBJS:.o=.d)

# Output binary
TARGET = forgedb

# ---- Targets ----

all: $(TARGET)

$(TARGET): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $(TARGET) $(OBJS)

# Generic rule: compile any src/*.cpp into src/*.o
%.o: %.cpp
	$(CXX) $(CXXFLAGS) -c $< -o $@

clean:
	rm -f src/*.o src/*.d $(TARGET)

# Auto-include generated dependency files (header change → recompile)
-include $(DEPS)

.PHONY: all clean
