/**
 * @file   CRucio.hpp
 * @brief  Contains the definition of CRucio, which manages simulated grid resources and CGridSite, which contains implementation of grid sites.
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * This file contains the definition of Rucio to manage grid resources. This is implemented in the CRucio class, which requires an implementation
 * of ISite to generate grid sites. CGridSite implements the ISite specialisation for grid sites.
 *
 */
#pragma once

#include <memory>
#include <string>
#include <vector>

#include "ISite.hpp"

#include "common/constants.h"
#include "common/IConfigConsumer.hpp"

class IRucioActionListener;
class CReaper;
struct SFile;
struct SReplica;


/**
* @brief ISite implementation that represents grid sites. Used by CRucio to create grid site objects.
*/
class CGridSite : public ISite
{
public:
    using ISite::ISite;

    virtual ~CGridSite();


    /**
    * @brief Implementation that creates instances of the native CStorageElement class.
    *
    * @param name name of the new storage element (string will be consumed)
    * @param allowDuplicateReplicas whether the created storage element is allowed to store only unique replicas or duplicates as well
    * @param limit storage limit of the new storage element
    *
    * @return a pointer to the newly created storage element object. The pointer is valid as long as the grid site is valid
    */
    auto CreateStorageElement(std::string&& name, bool allowDuplicateReplicas = false, SpaceType limit = 0) -> CStorageElement*;

    /**
    * @brief Generates an array containing pointers to all associated storage elements
    *
    * @return array with a pointer for each of the owned storage elements
    */
    auto GetStorageElements() const -> std::vector<CStorageElement*> override;

    /**
    * @brief array of the storage elements owned by this site
    */
    std::vector<std::unique_ptr<CStorageElement>> mStorageElements;
};


/**
* @brief Class representing the grid data management. It provides access to the grid sites and functionality to create files and free expired files.
*/
class CRucio : public IConfigConsumer
{
private:

    /**
    * @brief Pointer to a CReaper instance that can be used to remove expired files.
    */
    std::unique_ptr<CReaper> mReaper;

    /**
    * @brief Array of all file objects registered at the data managment
    */
    std::vector<std::unique_ptr<SFile>> mFiles;

public:

    /**
    * @brief Array of simulated grid sites
    */
    std::vector<std::unique_ptr<CGridSite>> mGridSites;


    /**
    * @brief Action interfaces registered for CRucio actions
    */
    std::vector<IRucioActionListener*> mActionListener;

    CRucio();
    ~CRucio();

    /**
    * @brief Method can be used to reserve storage in the internal file array to prevent numerous reallocations
    * 
    * @param amount number of elements to reserve in the file array
    */
    void ReserveFileSpace(std::size_t amount);

    /**
    * @brief Method that implements the creation of new files
    *
    * @param size the amount of storage the that a copy of this file requires
    * @param now the time the file was created at
    * @param lifetime the expected lifetime of the file
    * 
    * @return a pointer to the new file object. The pointer will be valid as long as the file exists.
    */
    auto CreateFile(SpaceType size, TickType now, TickType lifetime) -> SFile*;

    /**
    * @brief Removes a given file object with all its replicas from the data management
    *
    * @param file a valid pointer to the file object to remove
    * @param now the time the file was removed at
    */
    void RemoveFile(SFile* file, TickType now);

    /**
    * @brief Removes all files and all replicas from the data management
    * 
    * @param the time the files were removed at
    */
    void RemoveAllFiles(TickType now);


    /**
    * @brief Removes expired replicas of a given file from the data management
    * 
    * @param file the file whose replicas should be removed if expired
    * @param now the time to compare the expiration time with
    * 
    * @return the number of replicas removed
    */
    auto RemoveExpiredReplicasFromFile(SFile* file, TickType now) -> std::size_t;

    /**
    * @brief Extracts expired replicas of a given file from the data management
    *
    * @param file the file whose replicas should be removed if expired
    * @param now the time to compare the expiration time with
    *
    * @return a vector with pointers to the replicas that should be removed
    */
    auto ExtractExpiredReplicasFromFile(SFile* file, TickType now) -> std::vector<SReplica*>;

    /**
    * @brief Runs the associated CReaper to remove all expired files and their replicas
    * 
    * @param now the time to compare the expiration time with
    *
    * @return the number of files removed
    */
    auto RunReaper(TickType now) -> std::size_t;

    /**
    * @brief Getter for a const reference to the array of files
    *
    * @return an const reference to the array containing all registered files (copying could be expensive)
    */
    auto GetFiles() const -> const std::vector<std::unique_ptr<SFile>>&
    {return mFiles;}

    /**
    * @brief Method to create a grid site. Implemented by creating CGridSite objects.
    *
    * @param name name of the new grid site (string will be consumed)
    * @param locationName geographical location name of the grid site
    * @param multiLocationIdx index of the top level location if the site represents a multi regional site
    * 
    * @return a pointer to a CGridSite object that will be valid as long as CRucio is valid
    */
    auto CreateGridSite(std::string&& name, std::string&& locationName, std::uint8_t multiLocationIdx) -> CGridSite*;

    /**
    * @brief Helper function to find a storage element by its name. Iterates through all sites searching the storage element.
    *
    * @param name name of desired storage element
    *
    * @return a pointer to the desired CStorageElement or nullptr if not found
    */
    auto GetStorageElementByName(const std::string& name) const -> CStorageElement*;

    /**
    * @brief Initialises the grid infrastructure given a proper json config object, i.e., creates sites and storage elements as configured in the given json object
    *
    * @param config the json config object that contains all settings to use for setting up the grid infrastructure
    *
    * @return true if initialisation was successfull, false otherwise
    */
    bool LoadConfig(const json& config) final;
};
