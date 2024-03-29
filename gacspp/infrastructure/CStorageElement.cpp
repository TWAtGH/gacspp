#include <cassert>

#include "IActionListener.hpp"
#include "ISite.hpp"
#include "CNetworkLink.hpp"
#include "CRucio.hpp"
#include "CStorageElement.hpp"
#include "SFile.hpp"

#include "common/utils.hpp"

#include "output/COutput.hpp"


IStorageElementDelegate::IStorageElementDelegate(CStorageElement* storageElement, SpaceType limit)
    : mStorageElement(storageElement),
      mLimit(limit)
{}

IStorageElementDelegate::~IStorageElementDelegate() = default;



void CBaseStorageElementDelegate::OnOperation(CStorageElement::OPERATION op)
{
    (void)op;
}

auto CBaseStorageElementDelegate::CreateReplica(SFile* file, TickType now) -> SReplica*
{
    if(mLimit > 0 && (mUsedStorage + mAllocatedStorage + file->GetSize()) > mLimit)
        return nullptr;

    mAllocatedStorage += file->GetSize();
    mReplicas.emplace_back(std::make_unique<SReplica>(file, mStorageElement, now, mReplicas.size()));
    SReplica* newReplica = mReplicas.back().get();
    file->PostCreateReplica(newReplica);

    OnOperation(CStorageElement::INSERT);

    return newReplica;
}

void CBaseStorageElementDelegate::RemoveReplica(SReplica* replica, TickType now, bool needLock)
{
    (void)now;

    std::unique_lock<std::mutex> lock(mReplicaRemoveMutex, std::defer_lock);
    if (needLock)
        lock.lock();

    if (replica->mRemoveListener)
    {
        if (replica->mRemoveListener->PreRemoveReplica(replica, now) == false)
        {
            delete replica->mRemoveListener;
            replica->mRemoveListener = nullptr;
        }
    }

    

    replica->GetFile()->PreRemoveReplica(replica);

    const SpaceType curSize = replica->GetCurSize();
    const SpaceType allocationLeft = replica->GetFile()->GetSize() - curSize;
    const std::size_t idxToDelete = replica->mIndexAtStorageElement;

    assert(allocationLeft <= mAllocatedStorage);
    assert(curSize <= mUsedStorage);
    assert(idxToDelete < mReplicas.size());

    mAllocatedStorage -= allocationLeft;
    mUsedStorage -= curSize;

    mReplicas[idxToDelete] = std::move(mReplicas.back());
    mReplicas[idxToDelete]->mIndexAtStorageElement = idxToDelete;
    mReplicas.pop_back();

    OnOperation(CStorageElement::DELETE);
}

void CBaseStorageElementDelegate::OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now)
{
    (void)replica;
    (void)now;
    mUsedStorage += amount;

    assert(mAllocatedStorage >= amount);
    mAllocatedStorage -= amount;
}



auto CUniqueReplicaStorageElementDelegate::CreateReplica(SFile* file, TickType now) -> SReplica*
{
    if(file->GetReplicaByStorageElement(mStorageElement) != nullptr)
        return nullptr;

    return CBaseStorageElementDelegate::CreateReplica(file, now);
}



CStorageElement::CStorageElement(std::string&& name, ISite* site, bool allowDuplicateReplicas, SpaceType limit)
    : mId(GetNewId()),
      mName(std::move(name)),
      mSite(site)
{
    if(allowDuplicateReplicas)
        mDelegate.reset(new CBaseStorageElementDelegate(this, limit));
    else
        mDelegate.reset(new CUniqueReplicaStorageElementDelegate(this, limit));
}

void CStorageElement::OnOperation(OPERATION op)
{
    mDelegate->OnOperation(op);
}

auto CStorageElement::CreateNetworkLink(CStorageElement* dstStorageElement, SpaceType bandwidthBytesPerSecond) -> CNetworkLink*
{
    auto result = mDstStorageElementIdToNetworkLinkIdx.insert({ dstStorageElement->mId, mNetworkLinks.size() });
    assert(result.second);
    mNetworkLinks.emplace_back(std::make_unique<CNetworkLink>(bandwidthBytesPerSecond, this, dstStorageElement));
    return mNetworkLinks.back().get();
}

auto CStorageElement::CreateReplica(SFile* file, TickType now) -> SReplica*
{
    SReplica* replica = mDelegate->CreateReplica(file, now);
    
    if (!replica)
        return nullptr;

    for (IStorageElementActionListener* listener : mActionListener)
        listener->PostCreateReplica(replica, now);

    return replica;
}

void CStorageElement::RemoveReplica(SReplica* replica, TickType now, bool needLock)
{
    assert(this == replica->GetStorageElement());
    
    replica->mExpiresAt = now;

    for (IStorageElementActionListener* listener : mActionListener)
        listener->PreRemoveReplica(replica, now);

    mDelegate->RemoveReplica(replica, now, needLock);
}

void CStorageElement::OnIncreaseReplica(SReplica* replica, SpaceType amount, TickType now)
{
    mDelegate->OnIncreaseReplica(replica, amount, now);

    if (replica->IsComplete() && (amount > 0))
        for (IStorageElementActionListener* listener : mActionListener)
            listener->PostCompleteReplica(replica, now);
}

auto CStorageElement::GetNetworkLink(const CStorageElement* dstStorageElement) const -> CNetworkLink*
{
    auto result = mDstStorageElementIdToNetworkLinkIdx.find(dstStorageElement->mId);
    if (result == mDstStorageElementIdToNetworkLinkIdx.end())
        return nullptr;
    return mNetworkLinks[result->second].get();
}

auto CStorageElement::GetReplicas() const -> const std::vector<std::unique_ptr<SReplica>>&
{return mDelegate->GetReplicas(); }

auto CStorageElement::GetUsedStorage() const -> SpaceType
{return mDelegate->GetUsedStorage();}

auto CStorageElement::GetAllocatedStorage() const->SpaceType
{return mDelegate->GetAllocatedStorage();}

auto CStorageElement::GetLimit() const -> SpaceType
{return mDelegate->GetLimit();}

auto CStorageElement::GetUsedStorageLimitRatio() const -> double
{return mDelegate->GetUsedStorageLimitRatio();}

bool CStorageElement::CanStoreVolume(SpaceType volume) const
{return mDelegate->CanStoreVolume(volume);}