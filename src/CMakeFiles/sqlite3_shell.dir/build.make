# CMAKE generated file: DO NOT EDIT!
# Generated by "Unix Makefiles" Generator, CMake Version 2.8

#=============================================================================
# Special targets provided by cmake.

# Disable implicit rules so canonical targets will work.
.SUFFIXES:

# Remove some rules from gmake that .SUFFIXES does not remove.
SUFFIXES =

.SUFFIXES: .hpux_make_needs_suffix_list

# Suppress display of executed commands.
$(VERBOSE).SILENT:

# A target that is always out of date.
cmake_force:
.PHONY : cmake_force

#=============================================================================
# Set environment variables for the build.

# The shell in which to execute make rules.
SHELL = /bin/sh

# The CMake executable.
CMAKE_COMMAND = /usr/bin/cmake

# The command to remove a file.
RM = /usr/bin/cmake -E remove -f

# Escaping for special characters.
EQUALS = =

# The top-level source directory on which CMake was run.
CMAKE_SOURCE_DIR = /home/scanjee/cmu_db/proj1/sqlite-fall2017

# The top-level build directory on which CMake was run.
CMAKE_BINARY_DIR = /home/scanjee/cmu_db/proj1/sqlite-fall2017

# Include any dependencies generated for this target.
include src/CMakeFiles/sqlite3_shell.dir/depend.make

# Include the progress variables for this target.
include src/CMakeFiles/sqlite3_shell.dir/progress.make

# Include the compile flags for this target's objects.
include src/CMakeFiles/sqlite3_shell.dir/flags.make

src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o: src/CMakeFiles/sqlite3_shell.dir/flags.make
src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o: src/sqlite/shell.c
	$(CMAKE_COMMAND) -E cmake_progress_report /home/scanjee/cmu_db/proj1/sqlite-fall2017/CMakeFiles $(CMAKE_PROGRESS_1)
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Building C object src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o"
	cd /home/scanjee/cmu_db/proj1/sqlite-fall2017/src && /usr/bin/cc  $(C_DEFINES) $(C_FLAGS) -o CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o   -c /home/scanjee/cmu_db/proj1/sqlite-fall2017/src/sqlite/shell.c

src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.i: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Preprocessing C source to CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.i"
	cd /home/scanjee/cmu_db/proj1/sqlite-fall2017/src && /usr/bin/cc  $(C_DEFINES) $(C_FLAGS) -E /home/scanjee/cmu_db/proj1/sqlite-fall2017/src/sqlite/shell.c > CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.i

src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.s: cmake_force
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --green "Compiling C source to assembly CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.s"
	cd /home/scanjee/cmu_db/proj1/sqlite-fall2017/src && /usr/bin/cc  $(C_DEFINES) $(C_FLAGS) -S /home/scanjee/cmu_db/proj1/sqlite-fall2017/src/sqlite/shell.c -o CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.s

src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o.requires:
.PHONY : src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o.requires

src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o.provides: src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o.requires
	$(MAKE) -f src/CMakeFiles/sqlite3_shell.dir/build.make src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o.provides.build
.PHONY : src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o.provides

src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o.provides.build: src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o

# Object files for target sqlite3_shell
sqlite3_shell_OBJECTS = \
"CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o"

# External object files for target sqlite3_shell
sqlite3_shell_EXTERNAL_OBJECTS =

bin/sqlite3: src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o
bin/sqlite3: src/CMakeFiles/sqlite3_shell.dir/build.make
bin/sqlite3: lib/libsqlite3.so
bin/sqlite3: src/CMakeFiles/sqlite3_shell.dir/link.txt
	@$(CMAKE_COMMAND) -E cmake_echo_color --switch=$(COLOR) --red --bold "Linking C executable ../bin/sqlite3"
	cd /home/scanjee/cmu_db/proj1/sqlite-fall2017/src && $(CMAKE_COMMAND) -E cmake_link_script CMakeFiles/sqlite3_shell.dir/link.txt --verbose=$(VERBOSE)

# Rule to build all files generated by this target.
src/CMakeFiles/sqlite3_shell.dir/build: bin/sqlite3
.PHONY : src/CMakeFiles/sqlite3_shell.dir/build

src/CMakeFiles/sqlite3_shell.dir/requires: src/CMakeFiles/sqlite3_shell.dir/sqlite/shell.c.o.requires
.PHONY : src/CMakeFiles/sqlite3_shell.dir/requires

src/CMakeFiles/sqlite3_shell.dir/clean:
	cd /home/scanjee/cmu_db/proj1/sqlite-fall2017/src && $(CMAKE_COMMAND) -P CMakeFiles/sqlite3_shell.dir/cmake_clean.cmake
.PHONY : src/CMakeFiles/sqlite3_shell.dir/clean

src/CMakeFiles/sqlite3_shell.dir/depend:
	cd /home/scanjee/cmu_db/proj1/sqlite-fall2017 && $(CMAKE_COMMAND) -E cmake_depends "Unix Makefiles" /home/scanjee/cmu_db/proj1/sqlite-fall2017 /home/scanjee/cmu_db/proj1/sqlite-fall2017/src /home/scanjee/cmu_db/proj1/sqlite-fall2017 /home/scanjee/cmu_db/proj1/sqlite-fall2017/src /home/scanjee/cmu_db/proj1/sqlite-fall2017/src/CMakeFiles/sqlite3_shell.dir/DependInfo.cmake --color=$(COLOR)
.PHONY : src/CMakeFiles/sqlite3_shell.dir/depend
