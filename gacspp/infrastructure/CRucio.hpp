#pragma once

#include <memory>
#include <string>
#include <vector>

#include "ISite.hpp"

#include "common/constants.h"
#include "common/IConfigConsumer.hpp"

struct SFile;
struct SReplica;
class CReaper;


class CGridSite : public ISite
{
public:
    using ISite::ISite;

    std::vector<std::unique_ptr<CStorageElement>> mStorageElements;

    CGridSite(std::string&& name, std::string&& locationName);
    CGridSite(CGridSite&&) = default;
    CGridSite& operator=(CGridSite&&) = default;

    CGridSite(CGridSite const&) = delete;
    CGridSite& operator=(CGridSite const&) = delete;

    auto CreateStorageElement(std::string&& name, const TickType accessLatency) -> CStorageElement*;
};


class IFileActionListener
{
public:
    virtual void OnFileCreated(const TickType now, std::shared_ptr<SFile> file) = 0;
    virtual void OnFilesDeleted(const TickType now, const std::vector<std::weak_ptr<SFile>>& deletedFiles) = 0;
};


class IReplicaActionListener
{
public:
    virtual void OnReplicaCreated(const TickType now, std::shared_ptr<SReplica> replica) = 0;
    virtual void OnReplicasDeleted(const TickType now, const std::vector<std::weak_ptr<SReplica>>& deletedReplicas) = 0;
};


class CRucio : public IConfigConsumer
{
private:
    std::unique_ptr<CReaper> mReaper;

public:
    std::vector<std::shared_ptr<SFile>> mFiles;
    std::vector<std::unique_ptr<CGridSite>> mGridSites;

    std::vector<std::weak_ptr<IFileActionListener>> mFileActionListeners;
    std::vector<std::weak_ptr<IReplicaActionListener>> mReplicaActionListeners;

    CRucio();
    ~CRucio();

    auto CreateFile(const std::uint32_t size, const TickType now, const TickType lifetime) -> std::shared_ptr<SFile>;
    auto CreateGridSite(std::string&& name, std::string&& locationName, const std::uint8_t multiLocationIdx) -> CGridSite*;
    auto RunReaper(const TickType now) -> std::size_t;

    bool LoadConfig(const json& config) final;
};