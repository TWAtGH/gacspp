#include <iostream>

#include "CStorageElement.hpp"
#include "IActionListener.hpp"
#include "SFile.hpp"

#include "common/utils.hpp"


SFile::SFile(SpaceType size, TickType createdAt, TickType lifetime, std::size_t indexAtRucio)
    : mIndexAtRucio(indexAtRucio),
      mExpiresAt(createdAt+lifetime),
      mId(GetNewId()),
      mCreatedAt(createdAt),
      mSize(size)

{
    mReplicas.reserve(8);
}

void SFile::PostCreateReplica(SReplica* replica)
{
    mReplicas.push_back(replica);
}

void SFile::PreRemoveReplica(const SReplica * replica)
{
    std::size_t idx = 0;
    for (; idx < mReplicas.size(); ++idx)
    {
        if (mReplicas[idx] == replica)
        {
            mReplicas[idx] = mReplicas.back();
            mReplicas.pop_back();
            return;
        }
    }
    assert(false);
}

void SFile::ExtendExpirationTime(TickType newExpiresAt)
{
    if (newExpiresAt > mExpiresAt)
        mExpiresAt = newExpiresAt;
}

auto SFile::GetReplicaByStorageElement(const CStorageElement* storageElement) const -> SReplica*
{
    for (SReplica* replica : mReplicas)
        if (replica->GetStorageElement()->GetId() == storageElement->GetId())
            return replica;
    return nullptr;
}



SReplica::SReplica(SFile* file, CStorageElement* storageElement, TickType createdAt, std::size_t indexAtStorageElement)
    : mIndexAtStorageElement(indexAtStorageElement),
      mExpiresAt(file->mExpiresAt),
      mId(GetNewId()),
      mCreatedAt(createdAt),
      mFile(file),
      mStorageElement(storageElement)
{}

auto SReplica::Increase(SpaceType amount, TickType now) -> SpaceType
{
    SpaceType increment = std::min(amount, mFile->GetSize() - mCurSize);
    mCurSize += increment;
    mStorageElement->OnIncreaseReplica(this, increment, now);
    return increment;
}

void SReplica::ExtendExpirationTime(TickType newExpiresAt)
{
    if (newExpiresAt > mExpiresAt)
    {
        mExpiresAt = newExpiresAt;

        mFile->ExtendExpirationTime(newExpiresAt);
    }
}



bool SIndexedReplicas::AddReplica(SReplica* replica)
{
    auto res = mReplicaToIdx.insert({replica, mReplicas.size()});
    if(res.second)
    {
        mReplicas.push_back(replica);
        return true;
    }
    return false;
}

bool SIndexedReplicas::RemoveReplica(SReplica* replica)
{
    auto res = mReplicaToIdx.find(replica);
    if(res == mReplicaToIdx.end())
        return false;

    const std::size_t idx = res->second;
    
    SReplica* backReplica = mReplicas.back();
    mReplicas[idx] = backReplica;

    mReplicaToIdx.erase(res);
    mReplicas.pop_back();

    if (replica != backReplica)
        mReplicaToIdx[backReplica] = idx;

    return true;
}

bool SIndexedReplicas::RemoveReplica(std::size_t idx)
{
    return RemoveReplica(mReplicas[idx]);
}

auto SIndexedReplicas::ExtractBack() -> SReplica*
{
    assert(!mReplicas.empty());
    SReplica* back = mReplicas.back();
    RemoveReplica(back);
    return back;
}