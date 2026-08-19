Before building the project, ensure you have the following installed on your machine:

- **C++ Compiler:** Supporting C++17 or newer
  - **Windows:** Visual Studio 2019/2022 (MSVC) with "Desktop development with C++" workload
  - **Linux:** GCC 9+ or Clang 10+
  - **macOS:** Apple Clang / Xcode Command Line Tools
- **Build System:** [CMake](https://cmake.org/download/) (v3.15 or newer)
- **Version Control:** [Git](https://git-scm.com/)
- **Build Generators (Optional):** Ninja or GNU Make
- **Third-Party Libraries:** Managed automatically via CMake `FetchContent` (no manual library installation required).

---

## Getting Started

### 1. Clone the Repository

Clone the repository to your local machine using Git:

```bash
git clone https://github.com/Shinoruba/distributed-key-value-store.git
cd distributed-key-value-store
```

### 2. Build the Project

#### On Windows (PowerShell / Command Prompt)

Using CMake and MSVC:

```powershell
# Generate build files
cmake -B build -S .

# Build the executable (Debug or Release)
cmake --build build --config Release
```

*Alternatively, you can open the project folder directly in **Visual Studio** or **VS Code** with the CMake Tools extension.*

#### On Linux / macOS (Terminal)

```bash
# Generate build configuration
cmake -B build -S . -DCMAKE_BUILD_TYPE=Release

# Compile the project
cmake --build build -j $(nproc 2>/dev/null || sysctl -n hw.ncpu)
```

---

## Running the Project

Once the build finishes, run the generated binary:

### On Windows
```powershell
# From the project root
.\build\DistributedKVStore\Release\DistributedKVStore.exe
```
*(If built with a single-config generator like Ninja, the binary will be at `.\build\DistributedKVStore\DistributedKVStore.exe`)*

### On Linux / macOS
```bash
./build/DistributedKVStore/DistributedKVStore
```