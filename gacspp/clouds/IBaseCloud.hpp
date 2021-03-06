#pragma once

#include <string>
#include <memory>
#include <unordered_map>
#include <vector>

#include "common/constants.h"
#include "common/IConfigConsumer.hpp"

class ISite;
class CStorageElement;

class ICloudBill
{
public:
    virtual std::string ToString() const = 0;
};

class IBaseCloud : public IConfigConsumer
{
private:
    std::string mName;

public:
    std::vector<std::unique_ptr<ISite>> mRegions;

    IBaseCloud(std::string&& name);
    IBaseCloud(const IBaseCloud&) = delete;
    IBaseCloud& operator=(const IBaseCloud&) = delete;
    IBaseCloud(const IBaseCloud&&) = delete;
    IBaseCloud& operator=(const IBaseCloud&&) = delete;

    virtual ~IBaseCloud();

    virtual auto CreateRegion(std::string&& name,
                              std::string&& locationName,
                              std::uint8_t multiLocationIdx) -> ISite* = 0;

    virtual auto ProcessBilling(const TickType now) -> std::unique_ptr<ICloudBill> = 0;
    virtual void InitialiseNetworkLinks() = 0;

public:
    auto GetStorageElementByName(const std::string& name) const -> CStorageElement*;

    inline auto GetName() const -> const std::string&
    {return mName;}
};

class ICloudFactory
{
public:
    virtual ~ICloudFactory() = default;
    virtual auto CreateCloud(std::string&& cloudName) const -> std::unique_ptr<IBaseCloud> = 0;
};

class CCloudFactoryManager
{
private:
    CCloudFactoryManager() = default;
    CCloudFactoryManager(const CCloudFactoryManager&) = delete;
    CCloudFactoryManager& operator=(const CCloudFactoryManager&) = delete;
    CCloudFactoryManager(const CCloudFactoryManager&&) = delete;
    CCloudFactoryManager& operator=(const CCloudFactoryManager&&) = delete;

    std::unordered_map<std::string, std::unique_ptr<ICloudFactory>> mCloudFactories;

public:
    static auto GetRef() -> CCloudFactoryManager&;

    void AddFactory(std::string&& cloudId, std::unique_ptr<ICloudFactory>&& factory);
    void RemoveFactory(const std::string& cloudId);
    auto CreateCloud(const std::string& cloudId, std::string&& cloudName) const -> std::unique_ptr<IBaseCloud>;
};
