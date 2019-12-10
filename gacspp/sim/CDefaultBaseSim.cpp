#include <cassert>
#include <iostream>

#include "CDefaultBaseSim.hpp"

#include "clouds/gcp/CCloudGCP.hpp" //todo: should be removed

#include "common/CConfigManager.hpp"

#include "infrastructure/CNetworkLink.hpp"
#include "infrastructure/CRucio.hpp"

#include "output/COutput.hpp"

#include "third_party/nlohmann/json.hpp"



bool CDefaultBaseSim::SetupRucio(const json& profileJson)
{
    bool success = true;
    CConfigManager& configManager = CConfigManager::GetRef();

    mRucio = std::make_unique<CRucio>();

    try
    {
        json rucioCfg;
        configManager.TryLoadProfileCfg(rucioCfg, configManager.GetFileNameFromObj(profileJson.at("rucio")));
        if(!mRucio->LoadConfig(rucioCfg))
        {
            std::cout<<"Failed to apply config to Rucio!"<<std::endl;
            mRucio.reset(nullptr);
            success = false;
        }
    }
    catch(const json::out_of_range& error)
    {
        std::cout << "Failed to load Rucio cfg: " << error.what() << std::endl;
        success = false;
    }

    return success;
}

bool CDefaultBaseSim::SetupClouds(const json& profileJson)
{
    bool success = true;
    CConfigManager& configManager = CConfigManager::GetRef();

    try
    {
        for (const json& cloudJson : profileJson.at("clouds"))
        {
            std::unique_ptr<IBaseCloud> cloud;
            try
            {
                const std::string cloudId = cloudJson.at("id").get<std::string>();
                cloud = CCloudFactoryManager::GetRef().CreateCloud(cloudId, cloudJson.at("name").get<std::string>());

                if (!cloud)
                {
                    std::cout << "Failed to create cloud with id: " << cloudId << std::endl;
                    success = false;
                    continue;
                }
            }
            catch (const json::exception& error)
            {
                std::cout << "Failed to load config for cloud: " << error.what() << std::endl;
                success = false;
                continue;
            }

            json cloudCfg;
            configManager.TryLoadProfileCfg(cloudCfg, configManager.GetFileNameFromObj(cloudJson));
            if(!cloud->LoadConfig(cloudCfg))
            {
                std::cout<<"Failed to apply config to cloud: "<<cloud->GetName()<<std::endl;
                success = false;
            }
            else
                mClouds.emplace_back(std::move(cloud));
        }
    }
    catch (const json::out_of_range& error)
    {
        std::cout << "Failed to load clouds cfg: " << error.what() << std::endl;
        success = false;
    }

    return success;
}

bool CDefaultBaseSim::AddGridToOutput()
{
    bool success = true;
    COutput& output = COutput::GetRef();
    std::string row;

    //add all grid sites and storage elements to output DB (before links)
    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
        assert(gridSite);

        row = std::to_string(gridSite->GetId()) + ",";
        row += "'" + gridSite->GetName() + "',";
        row += "'" + gridSite->GetLocationName() + "',";
        row += "'grid'";

        success = success && output.InsertRow("Sites", row);

        for(const std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
        {
            assert(storageElement);

            row = std::to_string(storageElement->GetId()) + ",";
            row += std::to_string(gridSite->GetId()) + ",";
            row += "'" + storageElement->GetName() + "'";

            success = success && output.InsertRow("StorageElements", row);
        }
    }

    return success;
}

bool CDefaultBaseSim::AddCloudsToOutput()
{
    bool success = true;
    COutput& output = COutput::GetRef();
    std::string row;

    //add all cloud regions and buckets to output DB and then create and add all links
    for(const std::unique_ptr<IBaseCloud>& cloud : mClouds)
    {
        assert(cloud);

        for(const std::unique_ptr<ISite>& cloudSite : cloud->mRegions)
        {
            assert(cloudSite);

            row = std::to_string(cloudSite->GetId()) + ",";
            row += "'" + cloudSite->GetName() + "',";
            row += "'" + cloudSite->GetLocationName() + "',";
            row += "'" + cloud->GetName() + "'";

            success = success && output.InsertRow("Sites", row);

            //todo: change to be cloud impl independent
            const gcp::CRegion* region = dynamic_cast<gcp::CRegion*>(cloudSite.get());
            assert(region);
            for(const std::unique_ptr<gcp::CBucket>& bucket : region->mStorageElements)
            {
                assert(bucket);

                row = std::to_string(bucket->GetId()) + ",";
                row += std::to_string(cloudSite->GetId()) + ",";
                row += "'" + bucket->GetName() + "'";

                success = success && output.InsertRow("StorageElements", row);
            }
        }
    }

    return success;
}

bool CDefaultBaseSim::SetupLinks(const json& profileJson)
{
    CConfigManager& configManager = CConfigManager::GetRef();

    json linksCfg;
    if(!configManager.TryLoadProfileCfg(linksCfg, configManager.GetFileNameFromObj(profileJson.at("links"))))
        return false;

    assert(mRucio);

    std::unordered_map<std::string, CStorageElement*> nameToStorageElement;
    bool success = true;

    for(const std::unique_ptr<CGridSite>& gridSite : mRucio->mGridSites)
    {
        assert(gridSite);
        for (const std::unique_ptr<CStorageElement>& storageElement : gridSite->mStorageElements)
        {
            if (!nameToStorageElement.insert({ storageElement->GetName(), storageElement.get() }).second)
            {
                std::cout << "StorageElementName name is not unique: " << storageElement->GetName() << std::endl;
                success = false;
            }
        }
    }

    for(const std::unique_ptr<IBaseCloud>& cloud : mClouds)
    {
        assert(cloud);

        for(const std::unique_ptr<ISite>& cloudSite : cloud->mRegions)
        {
            assert(cloudSite);
            std::vector<CStorageElement*> storageElements;
            cloudSite->GetStorageElements(storageElements);
            for (CStorageElement* storageElement : storageElements)
            {
                if (!nameToStorageElement.insert({ storageElement->GetName(), storageElement }).second)
                {
                    std::cout << "CloudBucket name is not unique: " << cloud->GetName() << ": " << storageElement->GetName() << std::endl;
                    success = false;
                }
            }
        }
    }

    if(!success)
        return false;

    COutput& output = COutput::GetRef();
    std::string row;

    try
    {
        for(const auto& [srcStorageElementName, dstNameCfgJson] : linksCfg.items())
        {
            CStorageElement* srcStorageElement;
            try
            {
                srcStorageElement = nameToStorageElement.at(srcStorageElementName);
            }
            catch (std::out_of_range& error)
            {
                std::cout << "Failed to find src storage element for link configuration: " << srcStorageElementName << std::endl;
                success = false;
                continue;
            }

            for(const auto& [dstStorageElementName, dstLinkCfgJson] : dstNameCfgJson.items())
            {
                CStorageElement* dstStorageElement;
                try
                {
                    dstStorageElement = nameToStorageElement.at(dstStorageElementName);
                }
                catch (std::out_of_range& error)
                {
                    std::cout << "Failed to find dst storage element for link configuration: " << dstStorageElementName << std::endl;
                    success = false;
                    continue;
                }

                try
                {
                    SpaceType bandwidth = dstLinkCfgJson.at("bandwidth").get<SpaceType>();
                    CNetworkLink* link = srcStorageElement->CreateNetworkLink(dstStorageElement, bandwidth);

                    row = std::to_string(link->GetId()) + ",";
                    row += std::to_string(link->GetSrcStorageElement()->GetId()) + ",";
                    row += std::to_string(link->GetDstStorageElement()->GetId());

                    success = success && output.InsertRow("NetworkLinks", row);

                    if(dstLinkCfgJson.at("receivingLink").empty())
                        continue;

                    bandwidth = dstLinkCfgJson.at("receivingLink").at("bandwidth").get<SpaceType>();
                    link = dstStorageElement->CreateNetworkLink(srcStorageElement, bandwidth);

                    row = std::to_string(link->GetId()) + ",";
                    row += std::to_string(link->GetSrcStorageElement()->GetId()) + ",";
                    row += std::to_string(link->GetDstStorageElement()->GetId());

                    success = success && output.InsertRow("NetworkLinks", row);
                }
                catch (const json::out_of_range& error)
                {
                    std::cout << "Failed to create link: " << error.what() << std::endl;
                    success = false;
                }
            }
        }
    }
    catch (const json::out_of_range& error)
    {
        std::cout << "Failed to load links cfg: " << error.what() << std::endl;
        success = false;
    }

    for(const std::unique_ptr<IBaseCloud>& cloud : mClouds)
        cloud->InitialiseNetworkLinks();

    return success;
}

bool CDefaultBaseSim::SetupDefaults(const json& profileJson)
{
    if(!SetupRucio(profileJson))
        return false;
    if(!SetupClouds(profileJson))
        return false;

    if(!AddGridToOutput())
        return false;
    if(!AddCloudsToOutput())
        return false;

    if(!SetupLinks(profileJson))
        return false;

    return true;
}
