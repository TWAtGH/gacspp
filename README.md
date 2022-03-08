# Grid and Cloud Simulation C++ (gacspp)

## Overview
GACSPP is a discrete event based simulation, which allows evaluating models combining grid and cloud storage resources.
The simulation was used to evaluate the Hot/Cold Storage Data Carousel (HCDC) model.

## Dependencies
The simulation was developed, tested, and used on CentOS8. However, it can also be builded and used on windows based platforms.

The code makes uses of certain C++17 features from the STL, e.g., filesystem access. Therefore, the provided Makefile uses the `-std=c++17` option.

The [nlohmann library](https://github.com/nlohmann/json) is another used essential dependency. However, it consists of header files only and is included in this repository as third_party library.

The preferred output system is based on [PostgreSQL](https://www.postgresql.org/). This requires [libpq](https://www.postgresql.org/docs/11/libpq.html). During development, libpq version 11 was used
but higher versions should work as well.

By default, no output system is activated and only very basic information are printed to stdout. To enable PostgreSQL output, the preprocessor constant `WITH_PSQL` must be defined.
Furthermore, the path to `libpq-fe.h` must be known to the compiler and the linker must be able to resolve `-lpq`. Finally, during runtime a PostgreSQL server must be provided.