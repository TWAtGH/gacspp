#include <cassert>

#include "ISite.hpp"
#include "CNetworkLink.hpp"
#include "CStorageElement.hpp"
#include "SFile.hpp"

#include "output/COutput.hpp"


IStorageElementDelegate::IStorageElementDelegate(CStorageElement* storageElement)
    : mStorageElement(storageElement)
{}

IStorageElementDelegate::~IStorageElementDelegate() = default;



void CBaseStorageElementDelegate::OnOperation(const CStorageElement::OPERATION op)
{
    (void)op;
}

auto CBaseStorageElementDelegate::CreateReplica(std::shared_ptr<SFile>& file, const TickType now) -> std::shared_ptr<SReplica>
{
    auto& replicas = GetStorageElement()->mReplicas;
    auto newReplica = std::make_shared<SReplica>(file, mStorageElement, now, replicas.size());
    file->mReplicas.emplace_back(newReplica);
    replicas.emplace_back(newReplica);

    OnOperation(CStorageElement::INSERT);

    return newReplica;
}

void CBaseStorageElementDelegate::OnIncreaseReplica(const SpaceType amount, const TickType now)
{
    (void)now;
    mUsedStorage += amount;
}

void CBaseStorageElementDelegate::OnRemoveReplica(const SReplica* replica, const TickType now, bool needLock)
{
    (void)now;
    auto& replicas = GetStorageElement()->mReplicas;
    const SpaceType curSize = replica->GetCurSize();
    const std::size_t idxToDelete = replica->mIndexAtStorageElement;
    std::shared_ptr<SReplica>& lastReplica = replicas.back();

    assert(curSize <= mUsedStorage);
    assert(idxToDelete < replicas.size());

    mUsedStorage -= curSize;

    std::size_t& idxLastReplica = lastReplica->mIndexAtStorageElement;
    if(idxToDelete != idxLastReplica)
    {
        idxLastReplica = idxToDelete;
        replicas[idxToDelete] = lastReplica;
    }
    replicas.pop_back();
    OnOperation(CStorageElement::DELETE);
}



auto CUniqueReplicaStorageElementDelegate::CreateReplica(std::shared_ptr<SFile>& file, const TickType now) -> std::shared_ptr<SReplica>
{
    const auto result = mFileIds.insert(file->GetId());

    if (!result.second)
        return nullptr;

    return CBaseStorageElementDelegate::CreateReplica(file, now);
}


void CUniqueReplicaStorageElementDelegate::OnRemoveReplica(const SReplica* const replica, const TickType now, bool needLock)
{
    std::unique_lock<std::mutex> lock(mReplicaRemoveMutex, std::defer_lock);
    if(needLock)
        lock.lock();

    auto ret = mFileIds.erase(replica->GetFile()->GetId());
    assert(ret == 1);

    CBaseStorageElementDelegate::OnRemoveReplica(replica, now, needLock);
}



CStorageElement::CStorageElement(std::string&& name, ISite* site, bool allowDuplicateReplicas)
    : mId(GetNewId()),
      mName(std::move(name)),
      mSite(site)
{
    if(allowDuplicateReplicas)
        mDelegate.reset(new CBaseStorageElementDelegate(this));
    else
        mDelegate.reset(new CUniqueReplicaStorageElementDelegate(this));
}

void CStorageElement::OnOperation(const OPERATION op)
{
    mDelegate->OnOperation(op);
}

auto CStorageElement::CreateNetworkLink(CStorageElement* const dstStorageElement, const SpaceType bandwidthBytesPerSecond) -> CNetworkLink*
{
    auto result = mDstSiteIdToNetworkLinkIdx.insert({ dstStorageElement->mId, mNetworkLinks.size() });
    assert(result.second);
    CNetworkLink* newNetworkLink = new CNetworkLink(bandwidthBytesPerSecond, this, dstStorageElement);
    mNetworkLinks.emplace_back(newNetworkLink);
    return newNetworkLink;
}

auto CStorageElement::CreateReplica(std::shared_ptr<SFile>& file, const TickType now) -> std::shared_ptr<SReplica>
{
    return mDelegate->CreateReplica(file, now);
}
void CStorageElement::OnIncreaseReplica(const SpaceType amount, const TickType now)
{
    mDelegate->OnIncreaseReplica(amount, now);
}
void CStorageElement::OnRemoveReplica(const SReplica* replica, const TickType now, const bool needLock)
{
    mDelegate->OnRemoveReplica(replica, now, needLock);
}

auto CStorageElement::GetNetworkLink(const CStorageElement* const dstStorageElement) -> CNetworkLink*
{
    auto result = mDstSiteIdToNetworkLinkIdx.find(dstStorageElement->mId);
    if (result == mDstSiteIdToNetworkLinkIdx.end())
        return nullptr;
    return mNetworkLinks[result->second].get();
}

auto CStorageElement::GetNetworkLink(const CStorageElement* const dstStorageElement) const -> const CNetworkLink*
{
    auto result = mDstSiteIdToNetworkLinkIdx.find(dstStorageElement->mId);
    if (result == mDstSiteIdToNetworkLinkIdx.end())
        return nullptr;
    return mNetworkLinks[result->second].get();
}
