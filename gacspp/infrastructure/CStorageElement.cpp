#include <cassert>

#include "ISite.hpp"
#include "CStorageElement.hpp"
#include "SFile.hpp"

#include "output/COutput.hpp"


CStorageElement::CStorageElement(std::string&& name, const TickType accessLatency, ISite* const site)
    : mId(GetNewId()),
      mName(std::move(name)),
      mAccessLatency(accessLatency),
      mSite(site)
{}

CStorageElement::~CStorageElement() = default;

void CStorageElement::OnOperation(const OPERATION op)
{
    (void)op;
}

auto CStorageElement::CreateReplica(std::shared_ptr<SFile>& file, const TickType now) -> std::shared_ptr<SReplica>
{
    const auto result = mFileIds.insert(file->GetId());

    if (!result.second)
        return nullptr;

    auto newReplica = std::make_shared<SReplica>(file, this, now, mReplicas.size());
    file->mReplicas.emplace_back(newReplica);
    mReplicas.emplace_back(newReplica);

    OnOperation(INSERT);

    return newReplica;
}

void CStorageElement::OnIncreaseReplica(const SpaceType amount, const TickType now)
{
    (void)now;
    mUsedStorage += amount;
}

void CStorageElement::OnRemoveReplica(const SReplica* const replica, const TickType now, bool needLock)
{
    (void)now;
    const SpaceType curSize = replica->GetCurSize();

    std::unique_lock<std::mutex> lock(mReplicaRemoveMutex, std::defer_lock);
    if(needLock)
        lock.lock();

    const std::size_t idxToDelete = replica->mIndexAtStorageElement;
    auto& lastReplica = mReplicas.back();
    auto ret = mFileIds.erase(replica->GetFile()->GetId());
    assert(ret == 1);
    assert(idxToDelete < mReplicas.size());
    assert(curSize <= mUsedStorage);

    mUsedStorage -= curSize;

    std::size_t& idxLastReplica = lastReplica->mIndexAtStorageElement;
    if(idxToDelete != idxLastReplica)
    {
        idxLastReplica = idxToDelete;
        mReplicas[idxToDelete] = lastReplica;
    }
    mReplicas.pop_back();
    OnOperation(DELETE);
}