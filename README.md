# Grid and Cloud Simulation C++ (gacspp)

## Overview
GACSPP is a discrete event based simulation, which allows evaluating models combining grid and cloud storage resources.
The simulation was used to evaluate the Hot/Cold Storage Data Carousel (HCDC) model.
See [Simulation and Evaluation of Cloud Storage Caching for Data Intensive Science](https://link.springer.com/article/10.1007/s41781-021-00076-w).

Also consider the automatically generated [code documentation](https://twatgh.github.io/)

## Dependencies
The simulation was developed, tested, and used on CentOS8. However, it can also be builded and used on windows based platforms.

The code makes uses of certain C++17 features from the STL, e.g., filesystem access. Therefore, the provided Makefile uses the `-std=c++17` option.

The [nlohmann library](https://github.com/nlohmann/json) is another used essential dependency. However, it consists of header files only and is included in this repository as third_party library.

The preferred output system is based on [PostgreSQL](https://www.postgresql.org/). This requires [libpq](https://www.postgresql.org/docs/11/libpq.html). During development, libpq version 11 was used
but higher versions should work as well.

By default, no output system is activated and detailed information are only printed to stdout. To enable PostgreSQL output, the preprocessor constant `WITH_PSQL` must be defined.
Furthermore, the path to `libpq-fe.h` must be known to the compiler and the linker must be able to resolve `-lpq`. Finally, a PostgreSQL server must be provided during runtime.

Having the dependencies in place compilation can simply be done using the provided Makefile.


## Configuration
GACSPP can be configured using json files. The default configuration can be investigated in the config/ folder.
The first file parsed by GACSPP is the `config/simconfig.json` file. This file has two important parts. It specifies
the profile name to use and the output system configuration.

Using the PostgreSQL backend, the output system will read the `"dbConnectionFile"` field in the output configuration section.
This field must either be configured with a proper PostgreSQL connection string or reference a file containing the connection string.
For example, by default the field points to the `psql_connection.json` file. This file can be created next to the `simconfig.json`
and configured with a connection string. The content of a proper connection string file would look like:
```
{
    "connectionStr": "host=localhost port=6603 dbname=gacspp user=admin password=secretdbpw"
}
```
Another important field is the `"dbInitFileName"` which points to a file that contains querys that should be executed after opening the connection
to a database and before closing the connection. By default the value points to the file `psql_init_default.json`.
The queries can be configured like this:
```
{
"init":[
    "DROP TABLE IF EXISTS Sites CASCADE;"
],

"shutdown":[
    "ALTER TABLE Sites ADD PRIMARY KEY(id);"
	]
}
```

After the output system has been started, the simulation will try to load the profile configured in the `simconfig.json`.
A profile names a subdirectory in the `profiles/` folder. For example, the `profiles/simEval` folder, which contains a scenario
that was used to evaluated the simulation. Each profile must have a `profile.json` in its directory. This file then contains
all configuration options for a certain simulation scenario.

The most important keys within a `profile.json` are:
- `"clouds"` allows specifying bucket and region configuration for clouds
- `"rucio"` used to configure the rucio module and the grid infrastructure
- `"links"` used to configure the network links

These configuration data for these keys are typically sourced out into extra files. This is done by using a special key name, e.g.,
```
"rucio": { "config": {"_file_": "rucio.json"} }
```

The config manager will resolve this and replace the value of `"config"` with the content of the `rucio.json` file.
The content of the `rucio.json` file describes the grid infrastructure like so:
```
{
"rucio":
{
    "sites":
    [
        {
            "multiLocationIdx": 2,
            "name": "Site-1",
            "location": "eu",
            "storageElements":
            [
                {
                    "name": "StorageElement-1",
                    "allowDuplicateReplicas": false
                }
            ]
        }
	]
}
}
```
This looks similar to the cloud config and the links config. The profile configurations in this repository can be used as examples.

However, there are more keys worth mentioning from the `profile.json`.
- `"maxTick"` defines the maximum time until the simulation will run
- `"dataGens"` a list of objects, where each object describes a data generator. The attributes from the example files are straightforwardly mapped to the property documentation of the code.
- `"transferCfgs"` a list of objects, where each objects contains configuration for a `"manager"` (transfer manager) and a `"generator"` (transfer generator)
The latter options make it possible to configure default implemented events via configuration files.
