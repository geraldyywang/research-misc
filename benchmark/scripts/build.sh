#!/bin/bash

# Exit on any error
set -e

# 1. Create a build directory (clean build if requested)
if [ "$1" == "clean" ]; then
    echo "Cleaning old build..."
    rm -rf build
fi
mkdir -p build
cd build

# 2. Configure the project
# We use -GNinja if installed (it's faster), otherwise default to Unix Makefiles
if command -v ninja >/dev/null 2>&1; then
    GENERATOR="-GNinja"
else
    GENERATOR="-G 'Unix Makefiles'"
fi

echo "Configuring project with CMake..."
cmake .. $GENERATOR \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

# 3. Compile
echo "Compiling..."
# nproc gets the number of CPU cores on your VM for parallel building
cmake --build . --config Release -j$(nproc)

echo "--------------------------------------"
echo "Build Successful!"
echo "Executables are in: ./build/"