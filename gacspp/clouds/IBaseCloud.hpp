/**
 * @file   IBaseCloud.hpp
 * @brief  Contains the interface definitions of cloud bills, clouds and the cloud factory to generate various cloud implementations.
 * 
 * @author Tobias Wegner
 * @date   March 2022
 * 
 * The three main classes here are ICloudBill, IBaseCloud and ICloudFactory. Theses classes allow implementing an arbitrary
 * cloud provider into the simulation. Use IBaseCloud to implement the logic of the custom cloud provider. The considered
 * output of a cloud can be wrapped into a custom ICloudBill implementation. To enable the simulation to generate objects
 * of the new cloud implementation use the ICloudFactory class and register its implementation using the CCloudFactoryManager singleton.
 * 
 */
#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/constants.h"
#include "common/IConfigConsumer.hpp"

class ISite;
class CStorageElement;

/**
* @brief Interface for a cloud bill
* 
* Wraps the output of a cloud during the simulation. Currently the final output is required to be only received as string.
*/
class ICloudBill
{
public:
   
    /**
     * @brief Converts the stored output data of the cloud into a string format.
     * 
     * @return string representing the output of the cloud
     */
    virtual std::string ToString() const = 0;
};


/**
* @brief Interface representing the base type for a custom cloud implementation.
* 
* Each cloud implementation is required to allow creating and accessing cloud regions,
* process the billing, and setup the network link objects.
*/
class IBaseCloud : public IConfigConsumer
{
private:
    /** The runtime name of the cloud. Will be set in the constructor and is read-only during runtime. */
    std::string mName;

public:
    /**
    * @brief The regions owned by this cloud. A region must implement the ISite interface.
    */
    std::vector<std::unique_ptr<ISite>> mRegions;

    /**
     * @brief Each cloud requires a name in order to be instanciated (string will be consumed).
     * 
     */
    IBaseCloud(std::string&& name);


    IBaseCloud(const IBaseCloud&) = delete;
    IBaseCloud& operator=(const IBaseCloud&) = delete;
    IBaseCloud(const IBaseCloud&&) = delete;
    IBaseCloud& operator=(const IBaseCloud&&) = delete;

    virtual ~IBaseCloud();

    /**
     * @brief Creates a new region for this cloud.
     *
     * @param name the name of the new region (string will be consumed)
     * @param locationName name of the location of the region (string will be consumed)
     * @param multiLocationIndex if the cloud provider supports transparent multi-regions
     * 
     * @return A pointer to the region as ISite type. The pointer is valid as long as the cloud is valid.
     */
    virtual auto CreateRegion(std::string&& name,
                              std::string&& locationName,
                              std::uint8_t multiLocationIdx) -> ISite* = 0;

    /**
     * @brief Processes the billing of this cloud.
     *
     * @param now the current simulation time
     *
     * @return Returns a  unique pointer to an instance of an ICloudBill implementation.
     */
    virtual auto ProcessBilling(const TickType now) -> std::unique_ptr<ICloudBill> = 0;


    /**
    * @brief Initialises the network links for this cloud. This is required as extra step because it must be done after all sites and storage elements have been created.
    */
    virtual void InitialiseNetworkLinks() = 0;

public:
    /**
     * @brief Helper function to get a storage element by its name. Iterates through all sites and their storage elements.
     *
     * @param name read-only name of the desired storage element
     *
     * @return Returns a pointer to the desired storage element or nullptr if not found.
     */
    auto GetStorageElementByName(const std::string& name) const -> CStorageElement*;

    /**
     * @brief Getter for the cloud name
     *
     * @return The name of the cloud
     */
    inline auto GetName() const -> const std::string&
    {return mName;}
};


/**
* @brief Interface a factory creating objects of a certain cloud implementation given the name of the cloud.
*/
class ICloudFactory
{
public:
    virtual ~ICloudFactory() = default;

    /**
     * @brief Getter for the cloud name
     *
     * @param name the name of the cloud to create (string will be consumed)
     * 
     * @return A unique pointer of the newly created cloud
     */
    virtual auto CreateCloud(std::string&& cloudName) const -> std::unique_ptr<IBaseCloud> = 0;
};


/** 
* @brief Manager allowing to register and use cloud factories.
* 
* The class is implemented as singleton. Use CCloudFactoryManager::GetRef() to retrieve a reference to its instance.
*/
class CCloudFactoryManager
{
private:
    CCloudFactoryManager() = default;

    CCloudFactoryManager(const CCloudFactoryManager&) = delete;
    CCloudFactoryManager& operator=(const CCloudFactoryManager&) = delete;
    CCloudFactoryManager(const CCloudFactoryManager&&) = delete;
    CCloudFactoryManager& operator=(const CCloudFactoryManager&&) = delete;

    /** Hashtable that maps a cloud id to the factory object to create an instance of that cloud, e.g., "gcp" -> gcp::CCloudFactory* */
    std::unordered_map<std::string, std::unique_ptr<ICloudFactory>> mCloudFactories;

public:
    /**
     * @brief Getter for object of the singleton
     * 
     * @return A reference to the only instance of this class
     */
    static auto GetRef() -> CCloudFactoryManager&;

    /**
     * @brief Registers a cloud factory in the factory manager
     *
     * @param cloudId the id of the cloud, e.g., "gcp" (string will be consumed)
     * @param factory the factory object used to create a cloud of the given id
     */
    void AddFactory(std::string&& cloudId, std::unique_ptr<ICloudFactory>&& factory);

    /**
     * @brief Removes the factory of the given id
     *
     * @param cloudId the id of the factory to remove
     */
    void RemoveFactory(const std::string& cloudId);

    /**
     * @brief Create a new cloud instance given the id of its factory and the name for the cloud.
     *
     * @param cloudId id of the cloud factory to use
     * @param cloudName name of the cloud that will be created (string will be consumed)
     *
     * @return A unique pointer of the newly created cloud. Will return nullptr if the given cloudId is not registered.
     */
    auto CreateCloud(const std::string& cloudId, std::string&& cloudName) const -> std::unique_ptr<IBaseCloud>;
};
