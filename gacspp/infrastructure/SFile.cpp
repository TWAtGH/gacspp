#include "CStorageElement.hpp"
#include "SFile.hpp"



SFile::SFile(const SpaceType size, const TickType createdAt, const TickType lifetime)
    : mExpiresAt(createdAt+lifetime),
      mId(GetNewId()),
      mCreatedAt(createdAt),
      mSize(size)

{
    mReplicas.reserve(8);
}

void SFile::Remove(const TickType now)
{
    for(const std::shared_ptr<SReplica>& replica : mReplicas)
        replica->OnRemoveByFile(now);
    mReplicas.clear();
}
void SFile::RemoveReplica(const TickType now, const std::shared_ptr<SReplica>& replica)
{
    std::size_t idx = 0;
    for(;idx<mReplicas.size();++idx)
    {
        if(mReplicas[idx] == replica)
        {
            replica->OnRemoveByFile(now);
            mReplicas[idx] = std::move(mReplicas.back());
            mReplicas.pop_back();
            return;
        }
    }
    assert(false);
}
auto SFile::RemoveExpiredReplicas(const TickType now) -> std::size_t
{
    const std::size_t numReplicas = mReplicas.size();

    if(numReplicas < 2)
    {
        mReplicas[0]->mExpiresAt = mExpiresAt;
        return 0; // do not delete last replica if file didnt expire
    }

    std::size_t frontIdx = 0;
    std::size_t backIdx = numReplicas - 1;

    while(backIdx > frontIdx && mReplicas[backIdx]->mExpiresAt <= now)
    {
        mReplicas[backIdx]->OnRemoveByFile(now);
        mReplicas.pop_back();
        --backIdx;
    }

    for(;frontIdx < backIdx; ++frontIdx)
    {
        std::shared_ptr<SReplica>& curReplica = mReplicas[frontIdx];
        if(curReplica->mExpiresAt <= now)
        {
            std::swap(curReplica, mReplicas[backIdx]);
            do
            {
                mReplicas[backIdx]->OnRemoveByFile(now);
                mReplicas.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && mReplicas[backIdx]->mExpiresAt <= now);
        }
    }

    if(backIdx == 0 && mReplicas.back()->mExpiresAt <= now)
    {
        mReplicas[backIdx]->OnRemoveByFile(now);
        mReplicas.pop_back();
    }
    return numReplicas - mReplicas.size();
}

auto SFile::ExtractExpiredReplicas(const TickType now, std::vector<std::shared_ptr<SReplica>>& expiredReplicas) -> std::size_t
{
    const std::size_t numReplicas = mReplicas.size();

    if(numReplicas < 2)
    {
        mReplicas[0]->mExpiresAt = mExpiresAt;
        return 0; // do not delete last replica if file didnt expire
    }

    std::size_t frontIdx = 0;
    std::size_t backIdx = numReplicas - 1;

    while(backIdx > frontIdx && mReplicas[backIdx]->mExpiresAt <= now)
    {
        mReplicas[backIdx]->OnRemoveByFile(now);
        expiredReplicas.emplace_back(std::move(mReplicas[backIdx]));
        mReplicas.pop_back();
        --backIdx;
    }

    for(;frontIdx < backIdx; ++frontIdx)
    {
        std::shared_ptr<SReplica>& curReplica = mReplicas[frontIdx];
        if(curReplica->mExpiresAt <= now)
        {
            std::swap(curReplica, mReplicas[backIdx]);
            do
            {
                mReplicas[backIdx]->OnRemoveByFile(now);
                expiredReplicas.emplace_back(std::move(mReplicas[backIdx]));
                mReplicas.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && mReplicas[backIdx]->mExpiresAt <= now);
        }
    }

    if(backIdx == 0 && mReplicas.back()->mExpiresAt <= now)
    {
        mReplicas[backIdx]->OnRemoveByFile(now);
        expiredReplicas.emplace_back(std::move(mReplicas[backIdx]));
        mReplicas.pop_back();
    }
    return numReplicas - mReplicas.size();
}

void SFile::ExtendExpirationTime(const TickType newExpiresAt)
{
    if (newExpiresAt > mExpiresAt)
        mExpiresAt = newExpiresAt;
}

auto SFile::GetReplicaByStorageElement(const CStorageElement* storageElement) -> std::shared_ptr<SReplica>
{
    for (std::shared_ptr<SReplica> replica : mReplicas)
        if (replica->GetStorageElement()->GetId() == storageElement->GetId())
            return replica;
    return std::shared_ptr<SReplica>();
}


SReplica::SReplica(std::shared_ptr<SFile>& file, CStorageElement* const storageElement, const TickType createdAt, const std::size_t indexAtStorageElement)
    : mIndexAtStorageElement(indexAtStorageElement),
      mExpiresAt(file->mExpiresAt),
      mId(GetNewId()),
      mCreatedAt(createdAt),
      mFile(file),
      mStorageElement(storageElement)
{}

auto SReplica::Increase(const SpaceType amount, const TickType now) -> SpaceType
{
    SpaceType increment = std::min(amount, mFile->GetSize() - mCurSize);
    mCurSize += increment;
    mStorageElement->OnIncreaseReplica(increment, now);
    return increment;
}

void SReplica::OnRemoveByFile(const TickType now)
{
    mStorageElement->OnRemoveReplica(this, now);
}

void SReplica::ExtendExpirationTime(const TickType newExpiresAt)
{
    if (newExpiresAt > mExpiresAt)
    {
        mExpiresAt = newExpiresAt;
        mFile->ExtendExpirationTime(newExpiresAt);
    }
}
