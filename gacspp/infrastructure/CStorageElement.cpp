#include <cassert>

#include "IActionListener.hpp"
#include "ISite.hpp"
#include "CNetworkLink.hpp"
#include "CRucio.hpp"
#include "CStorageElement.hpp"
#include "SFile.hpp"

#include "common/utils.hpp"

#include "output/COutput.hpp"


IStorageElementDelegate::IStorageElementDelegate(CStorageElement* storageElement, SpaceType quota)
    : mStorageElement(storageElement),
      mQuota(quota)
{}

IStorageElementDelegate::~IStorageElementDelegate() = default;



void CBaseStorageElementDelegate::OnOperation(const CStorageElement::OPERATION op)
{
    (void)op;
}

auto CBaseStorageElementDelegate::CreateReplica(std::shared_ptr<SFile> file, const TickType now) -> std::shared_ptr<SReplica>
{
    if(mQuota > 0 && (mUsedStorage + file->GetSize()) > mQuota)
        return nullptr;

    auto& replicas = GetStorageElement()->mReplicas;
    auto newReplica = std::make_shared<SReplica>(file, mStorageElement, now, replicas.size());
    file->mReplicas.emplace_back(newReplica);
    replicas.emplace_back(newReplica);

    OnOperation(CStorageElement::INSERT);

    //notify all listeners
    auto& listeners = GetStorageElement()->mReplicaActionListeners;
    for(std::size_t i=0; i<listeners.size();)
    {
        std::shared_ptr<IReplicaActionListener> listener = listeners[i].lock();
        if(!listener)
        {
            //remove invalid listeners
            listeners[i] = std::move(listeners.back());
            listeners.pop_back();
            continue;
        }

        listener->OnReplicaCreated(now, newReplica);

        ++i;
    }

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

    std::unique_lock<std::mutex> lock(mReplicaRemoveMutex, std::defer_lock);
    if (needLock)
        lock.lock();

    auto& replicas = GetStorageElement()->mReplicas;
    const SpaceType curSize = replica->GetCurSize();
    const std::size_t idxToDelete = replica->mIndexAtStorageElement;
    std::shared_ptr<SReplica>& lastReplica = replicas.back();

    assert(curSize <= mUsedStorage);
    assert(idxToDelete < replicas.size());

    //notify all listeners
    auto& listeners = GetStorageElement()->mReplicaActionListeners;
    for(std::size_t i=0; i<listeners.size();)
    {
        std::shared_ptr<IReplicaActionListener> listener = listeners[i].lock();
        if(!listener)
        {
            //remove invalid listeners
            listeners[i] = std::move(listeners.back());
            listeners.pop_back();
            continue;
        }

        listener->OnReplicaDeleted(now, replicas[idxToDelete]);

        ++i;
    }

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



auto CUniqueReplicaStorageElementDelegate::CreateReplica(std::shared_ptr<SFile> file, const TickType now) -> std::shared_ptr<SReplica>
{
    for (const std::shared_ptr<SReplica>& replica : file->mReplicas)
        if (replica->GetStorageElement()->GetId() == mStorageElement->GetId())
            return nullptr;

    return CBaseStorageElementDelegate::CreateReplica(file, now);
}



CStorageElement::CStorageElement(std::string&& name, ISite* site, bool allowDuplicateReplicas, SpaceType quota)
    : mId(GetNewId()),
      mName(std::move(name)),
      mSite(site)
{
    if(allowDuplicateReplicas)
        mDelegate.reset(new CBaseStorageElementDelegate(this, quota));
    else
        mDelegate.reset(new CUniqueReplicaStorageElementDelegate(this, quota));
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

auto CStorageElement::CreateReplica(std::shared_ptr<SFile> file, const TickType now) -> std::shared_ptr<SReplica>
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
