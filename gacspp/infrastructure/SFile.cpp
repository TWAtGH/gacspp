#include "CStorageElement.hpp"
#include "SFile.hpp"



SFile::SFile(const std::uint32_t size, const TickType expiresAt)
    : mExpiresAt(expiresAt),
      mId(GetNewId()),
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

auto SFile::RemoveExpiredReplicas(const TickType now) -> std::size_t
{
    const std::size_t numReplicas = mReplicas.size();

    if(numReplicas < 2)
        return 0; // do not delete last replica if file didnt expire

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



SReplica::SReplica(std::shared_ptr<SFile>& file, CStorageElement* const storageElement, const std::size_t indexAtStorageElement)
    : mIndexAtStorageElement(indexAtStorageElement),
      mExpiresAt(file->mExpiresAt),
      mId(GetNewId()),
      mFile(file),
      mStorageElement(storageElement)
{}

auto SReplica::Increase(std::uint32_t amount, const TickType now) -> std::uint32_t
{
    const std::uint32_t maxSize = mFile->GetSize();
    std::uint64_t newSize = mCurSize + amount;
    if (newSize >= maxSize)
    {
        amount = maxSize - mCurSize;
        newSize = maxSize;
    }
    mCurSize = static_cast<std::uint32_t>(newSize);
    mStorageElement->OnIncreaseReplica(amount, now);
    return amount;
}

void SReplica::OnRemoveByFile(const TickType now)
{
    mStorageElement->OnRemoveReplica(this, now);
}
