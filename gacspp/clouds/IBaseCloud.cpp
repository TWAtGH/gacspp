#include <cassert>

#include "IBaseCloud.hpp"
#include "ISite.hpp"

IBaseCloud::IBaseCloud(std::string&& name)
    : mName(std::move(name))
{}

IBaseCloud::~IBaseCloud() = default;

auto CCloudFactoryManager::GetRef() -> CCloudFactoryManager&
{
    static CCloudFactoryManager instance;
    return instance;
}

void CCloudFactoryManager::AddFactory(std::string&& cloudId, std::unique_ptr<ICloudFactory>&& factory)
{
    const auto result = mCloudFactories.insert({ std::move(cloudId), std::move(factory) });
    assert(result.second);
}

void CCloudFactoryManager::RemoveFactory(const std::string& cloudId)
{
    mCloudFactories.erase(cloudId);
}

auto CCloudFactoryManager::CreateCloud(const std::string& cloudId, std::string&& cloudName) const -> std::unique_ptr<IBaseCloud>
{
    const auto result = mCloudFactories.find(cloudId);
    if (result == mCloudFactories.cend())
        return nullptr;
    return result->second->CreateCloud(std::move(cloudName));
}
