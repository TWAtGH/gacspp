#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/constants.h"
#include "third_party/parallel_hashmap/phmap.h"

class ISite;
class CNetworkLink;
struct SFile;
struct SReplica;

class IStorageElementDelegate;


class CStorageElement
{
public:
    enum OPERATION
    {
        INSERT,
        GET,
        CREATE_TRANSFER,
        DELETE,
        CUSTOM
    };

    CStorageElement(std::string&& name, ISite* site, bool allowDuplicateReplicas = false);

    virtual void OnOperation(const OPERATION op);

    virtual auto CreateNetworkLink(CStorageElement* const dstStorageElement, const SpaceType bandwidth) -> CNetworkLink*;
    virtual auto CreateReplica(std::shared_ptr<SFile>& file, const TickType now) -> std::shared_ptr<SReplica>;
    virtual void OnIncreaseReplica(const SpaceType amount, const TickType now);
    virtual void OnRemoveReplica(const SReplica* replica, const TickType now, const bool needLock=true);

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetSite() const -> const ISite*
    {return mSite;}
    inline auto GetSite() -> ISite*
    {return mSite;}

    auto GetNetworkLink(const CStorageElement* const dstStorageElement) -> CNetworkLink*;
    auto GetNetworkLink(const CStorageElement* const dstStorageElement) const -> const CNetworkLink*;

    std::vector<std::unique_ptr<CNetworkLink>> mNetworkLinks;
    std::vector<std::shared_ptr<SReplica>> mReplicas;

private:
    IdType mId;
    std::string mName;

protected:
    ISite* mSite;
    std::unique_ptr<IStorageElementDelegate> mDelegate;

    std::unordered_map<IdType, std::size_t> mDstSiteIdToNetworkLinkIdx;
};



class IStorageElementDelegate
{
public:
    IStorageElementDelegate(CStorageElement* storageElement);

    IStorageElementDelegate(IStorageElementDelegate&&) = delete;
    IStorageElementDelegate& operator=(IStorageElementDelegate&&) = delete;
    IStorageElementDelegate(const IStorageElementDelegate&) = delete;
    IStorageElementDelegate& operator=(const IStorageElementDelegate&) = delete;

    virtual ~IStorageElementDelegate();


    virtual void OnOperation(const CStorageElement::OPERATION op) = 0;

    virtual auto CreateReplica(std::shared_ptr<SFile>& file, const TickType now) -> std::shared_ptr<SReplica> = 0;
    virtual void OnIncreaseReplica(const SpaceType amount, const TickType now) = 0;
    virtual void OnRemoveReplica(const SReplica* replica, const TickType now, bool needLock=true) = 0;

    inline auto GetStorageElement() -> CStorageElement*
    {return mStorageElement;}
    inline auto GetUsedStorage() -> SpaceType
    {return mUsedStorage;}

protected:
    CStorageElement *mStorageElement;
    SpaceType mUsedStorage = 0;
};



class CBaseStorageElementDelegate : public IStorageElementDelegate
{
public:
    using IStorageElementDelegate::IStorageElementDelegate;


    virtual void OnOperation(const CStorageElement::OPERATION op);

    virtual auto CreateReplica(std::shared_ptr<SFile>& file, const TickType now) -> std::shared_ptr<SReplica>;
    virtual void OnIncreaseReplica(const SpaceType amount, const TickType now);
    virtual void OnRemoveReplica(const SReplica* replica, const TickType now, bool needLock=true);

protected:
    std::mutex mReplicaRemoveMutex;
};



class CUniqueReplicaStorageElementDelegate : public CBaseStorageElementDelegate
{
public:
    using CBaseStorageElementDelegate::CBaseStorageElementDelegate;


    virtual auto CreateReplica(std::shared_ptr<SFile>& file, const TickType now) -> std::shared_ptr<SReplica>;
};
