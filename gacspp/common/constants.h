#pragma once

#include <cstdint>
#include <random>


#define JSON_DUMP_SPACES (2)
#define JSON_FILE_IMPORT_KEY ("_file_")

// comment out to use dynamic names
//#define STATIC_DB_NAME (":memory:")
//#define STATIC_DB_LOG_NAME ("sqlitedb.log")

#define PI (3.14159265359)

#define ONE_MiB (1048576.0) //2^20
#define ONE_GiB (1073741824.0) // 2^30
#define BYTES_TO_MiB(x) ((x) / ONE_MiB)
#define BYTES_TO_GiB(x) ((x) / ONE_GiB)
#define MiB_TO_BYTES(x) ((x) * ONE_MiB)
#define GiB_TO_BYTES(x) ((x) * ONE_GiB)

#define SECONDS_PER_DAY (86400.0) // 60 * 60 * 24
#define SECONDS_PER_MONTH (SECONDS_PER_DAY * 30.0)
#define SECONDS_TO_MONTHS(x) ((x)/SECONDS_PER_MONTH)
#define DAYS_TO_SECONDS(x) ((x) * SECONDS_PER_DAY)

typedef std::minstd_rand RNGEngineType;

typedef std::uint64_t SpaceType;
typedef std::uint64_t TickType;
typedef std::uint64_t IdType;
inline IdType GetNewId()
{
    static IdType id = 0;
    return ++id;
}
