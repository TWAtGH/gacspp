/**
 * @file   CDefaultBaseSim.hpp
 * @brief  Provides a basic implementation of the simulation engine interface
 *
 * @author Tobias Wegner
 * @date   March 2022
 *
 * This file contains the implementation of the default implemented simulation engine. This
 * simulation engine can be set up using solely default implemented events and configurations files.
 *
 */

#pragma once

#include "IBaseSim.hpp"


class CBaseTransferManager;
class CBufferedOnDeletionInsert;

/**
* @brief Default implementation for a simulation engine
*
* Simulation engine that allows being setup using configuration files. The SetupDefaults() method will be
* called by the user and subsequently call its methods in order to set up the simulation.
*/
class CDefaultBaseSim : public IBaseSim
{
public:
    /**
    * @brief Set up the default simulation using the given json config. Must be called by the user prior to Run().
    *
    * @param profileJson the full profile configuration, containing all configuration data about the simulated model
    *
    * @return true on successfull initialisation or false otherwise
    */
    bool SetupDefaults(const json& profileJson) override;

    /**
    * @brief First method called by SetupDefaults(). Used to create a CRucio instance and load its configuration.
    *
    * @param profileJson the full profile configuration, containing all configuration data about the simulated model
    *
    * @return true on successfull initialisation or false otherwise
    */
    virtual bool SetupRucio(const json& profileJson);

    /**
    * @brief Called by SetupDefaults() after Rucio has been created. Creates and configures all required cloud instances.
    *
    * @param profileJson the full profile configuration, containing all configuration data about the simulated model
    *
    * @return true on successfull initialisation or false otherwise
    */
    virtual bool SetupClouds(const json& profileJson);

    /**
    * @brief Called by SetupDefaults() after Rucio and Clouds have been loaded. Adds all created infrastructure objects to the output system.
    * 
    * @return true on successfull initialisation or false otherwise
    */
    virtual bool AddGridToOutput();

    /**
    * @brief Called by SetupDefaults() after Rucio and Clouds have been loaded. Adds all created cloud objects to the output system.
    *
    * @return true on successfull initialisation or false otherwise
    */
    virtual bool AddCloudsToOutput();

    /**
    * @brief Last method called by SetupDefaults(). Sets up all network links between storage endpoints and adds them to the output system.
    *
    * @param profileJson the full profile configuration, containing all configuration data about the simulated model
    *
    * @return true on successfull initialisation or false otherwise
    */
    virtual bool SetupLinks(const json& profileJson);


    /**
    * @brief Creates an implementation of CBaseTransferManager given a configuration object
    *
    * @param transferManagerCfg the configuration data specifying the type and settings of the transfer manager to create
    *
    * @return a shared pointer to an implementation of CBaseTransferManager on success or nullptr otherwise
    */
    virtual auto CreateTransferManager(const json& transferManagerCfg) const -> std::shared_ptr<CBaseTransferManager>;


    /**
    * @brief Creates a known transfer generator given a configuration object and associates it with the given transfer manager
    *
    * @param transferGenCfg the configuration data specifying the type and settings of the transfer generator to create
    * @param transferManager a shared pointer to the transfer manager that will be used by the transfer generator
    *
    * @return a shared pointer to a CSchedulable that implements a transfer generator. Can return nullptr on failure
    */
    virtual auto CreateTransferGenerator(const json& transferGenCfg, const std::shared_ptr<CBaseTransferManager>& transferManager) -> std::shared_ptr<CSchedulable>;

private:
    /**
    * @brief Action interface listener that writes objects to the output system before they are deleted.
    */
    std::shared_ptr<CBufferedOnDeletionInsert> mDeletionInserter;
};
