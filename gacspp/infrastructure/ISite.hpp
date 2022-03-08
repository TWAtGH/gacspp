/**
 * @file   ISite.hpp
 * @brief  Contains the definition of the ISite interface
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * This file contains the definition of the ISite interface used to represent resource sites.
 *
 */
#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/constants.h"

class CStorageElement;
class IStorageElementDelegate;



/**
* @brief Interface for a storage resource site
*
* A site object represents a general data centre. This can be a grid data centre (grid site) or a cloud data centre (region).
* Sites have a certain geographic location and conists of multiple storage elements. A class implementing this interface must
* implement functionality to create, store, and access storage elements.
*/
class ISite
{
public:
    /**
    * @brief Constructor initialise the base data for the site
    * 
    * @param name name of the site (string will be consumed)
    * @param locationName name of the geographic location of the site
    * @param multiLocationIdx index of the top level location if this site represents a multi regional site
    */
    ISite(std::string&& name, std::string&& locationName, std::uint8_t multiLocationIdx);

    ISite(ISite&&) = delete;
    ISite& operator=(ISite&&) = delete;
    ISite(const ISite&) = delete;
    ISite& operator=(const ISite&) = delete;

    virtual ~ISite();


    inline bool operator==(const ISite& b) const
    {return mId == b.mId;}
    inline bool operator!=(const ISite& b) const
    {return mId != b.mId;}

    /**
    * @brief Method declaration to create a new storage element assoicated to this site
    *
    * @param name name of the new storage element (string will be consumed)
    * @param allowDuplicateReplicas whether the created storage element is allowed to store only unique replicas or duplicates as well
    * @param limit storage limit of the new storage element
    * 
    * @return a pointer to the newly created storage element object. The pointer should be kept valid as long as its owning site is valid
    */
    virtual auto CreateStorageElement(std::string&& name, bool allowDuplicateReplicas = false, SpaceType limit = 0) -> CStorageElement* = 0;

    /**
    * @brief Method to get all pointers to the storage elements associated with this site.
    *
    * @return array with a pointer for each of the owned storage elements
    */
    virtual auto GetStorageElements() const -> std::vector<CStorageElement*> = 0;


    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetLocationName() const -> const std::string&
    {return mLocationName;}
    inline auto GetMultiLocationIdx() const -> std::uint8_t
    {return mMultiLocationIdx;}


private:
    /**
    * @brief Unique id of this site. Id is unique accross all object types
    */
    IdType mId;

    /**
    * @brief Name of the site
    */
    std::string mName;

    /**
    * @brief Geographic location name of the site
    */
    std::string mLocationName;

    /**
    * @brief Index of the top level location if this site represents a multi regional site
    */
    std::uint8_t mMultiLocationIdx;

public:

    /**
    * @brief Attributes that were not consumed from the config during loading will be stored in this hashtable
    */
    std::unordered_map<std::string, std::string> mCustomConfig;
};
