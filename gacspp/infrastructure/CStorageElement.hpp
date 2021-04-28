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
class IStorageElementActionListener;


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

    CStorageElement(std::string&& name, ISite* site, bool allowDuplicateReplicas = false, SpaceType limit = 0);

    virtual void OnOperation(OPERATION op);

    virtual auto CreateNetworkLink(CStorageElement* dstStorageElement, SpaceType bandwidth) -> CNetworkLink*;

    virtual auto CreateReplica(SFile* file, TickType now) -> SReplica*;
    virtual void RemoveReplica(SReplica* replica, TickType now, bool needLock = true);
    virtual void OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now);

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetSite() const -> ISite*
    {return mSite;}

    auto GetReplicas() const -> const std::vector<std::unique_ptr<SReplica>>&;

    inline auto GetNetworkLinks() const -> const std::vector<std::unique_ptr<CNetworkLink>>&
    {return mNetworkLinks;}
    auto GetNetworkLink(const CStorageElement* dstStorageElement) const -> CNetworkLink*;

    auto GetUsedStorage() const->SpaceType;
    auto GetAllocatedStorage() const->SpaceType;
    auto GetLimit() const -> SpaceType;
    auto GetUsedStorageLimitRatio() const -> double;
    bool CanStoreVolume(SpaceType volume) const;

    std::vector<IStorageElementActionListener*> mActionListener;

    std::unique_ptr<class IValueGenerator> mAccessLatency;
    
private:
    IdType mId;
    std::string mName;

    std::vector<std::unique_ptr<CNetworkLink>> mNetworkLinks;

protected:
    ISite* mSite;
    std::unique_ptr<IStorageElementDelegate> mDelegate;

    std::unordered_map<IdType, std::size_t> mDstSiteIdToNetworkLinkIdx;
};



class IStorageElementDelegate
{
public:
    IStorageElementDelegate(CStorageElement* storageElement, SpaceType limit = 0);

    IStorageElementDelegate(IStorageElementDelegate&&) = delete;
    IStorageElementDelegate& operator=(IStorageElementDelegate&&) = delete;
    IStorageElementDelegate(const IStorageElementDelegate&) = delete;
    IStorageElementDelegate& operator=(const IStorageElementDelegate&) = delete;

    virtual ~IStorageElementDelegate();


    virtual void OnOperation(CStorageElement::OPERATION op) = 0;

    virtual auto CreateReplica(SFile* file, TickType now)->SReplica* = 0;
    virtual void RemoveReplica(SReplica* replica, TickType now, bool needLock = true) = 0;
    virtual void OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now) = 0;

    inline auto GetReplicas() const -> const std::vector<std::unique_ptr<SReplica>>&
    {return mReplicas;}
    inline auto GetStorageElement() const -> CStorageElement*
    {return mStorageElement;}

    inline auto GetUsedStorage() const -> SpaceType
    {return mUsedStorage;}
    inline auto GetAllocatedStorage() const -> SpaceType
    {return mAllocatedStorage;}
    inline auto GetLimit() const -> SpaceType
    {return mLimit;}
    inline auto GetUsedStorageLimitRatio() const -> double
    {return (mLimit > 0) ? static_cast<double>(mUsedStorage) / mLimit : 0;}
    inline bool CanStoreVolume(SpaceType volume) const
    {return (mLimit > 0) ? ((mUsedStorage + mAllocatedStorage + volume) <= mLimit) : true;}

protected:
    std::vector<std::unique_ptr<SReplica>> mReplicas;
    CStorageElement *mStorageElement;
    SpaceType mUsedStorage = 0;
    SpaceType mAllocatedStorage = 0;
    SpaceType mLimit;
};



class CBaseStorageElementDelegate : public IStorageElementDelegate
{
public:
    using IStorageElementDelegate::IStorageElementDelegate;


    void OnOperation(CStorageElement::OPERATION op) override;

    auto CreateReplica(SFile* file, TickType now)->SReplica* override;
    void RemoveReplica(SReplica* replica, TickType now, bool needLock = true) override;
    void OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now) override;

protected:
    std::mutex mReplicaRemoveMutex;
};



class CUniqueReplicaStorageElementDelegate : public CBaseStorageElementDelegate
{
public:
    using CBaseStorageElementDelegate::CBaseStorageElementDelegate;


    auto CreateReplica(SFile* file, TickType now) -> SReplica* override;
};
