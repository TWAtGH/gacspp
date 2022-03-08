/**
 * @file   constants.h
 * @brief  Provides commonly used constants and typedefs
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * The most important typedefs used in gacspp are SpaceType, TickType, and IdType. SpaceType
 * is used to indicate that the variable will store values related to storage or data, e.g.,
 * file sizes, storage volumes, or bandwidths.
 * TickType is used to indicate that a variable is used to store values related to simulation
 * time points.
 * IdType is used for variables that store object IDs.
 * By default, all three types are mapped to 64 bit unsigned integers.
 *
 */

#pragma once

#include <chrono>
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

typedef std::chrono::high_resolution_clock::time_point TimePointType;
typedef std::chrono::duration<double> DurationType;

/**
* @brief Type used to store values related to storage or data volumes.
*/
typedef std::uint64_t SpaceType;

/**
* @brief Type used to store values related to the simulation time.
*/
typedef std::uint64_t TickType;

/**
* @brief Type used to store object IDs.
*/
typedef std::uint64_t IdType;


/*! \mainpage GACSPP Index
 *
 * \section intro_sec Introduction
 *
 * This is the automatically generated documentation of GACSPP.
 * The program entry point is at gacspp/sim/gacspp.cpp
 * There the CConfigManager will be instanciated to subsequently load config files, initialise the output system, and load a simulation profile.
 * Please also consider the first step infos provide at github: https://github.com/TWAtGH/gacspp
 * 
 * You can use the navigation on this site to get an overview of the existing files in GACSPP and to explore the used classes.
 */