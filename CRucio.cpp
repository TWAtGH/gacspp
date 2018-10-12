#include "CRucio.hpp"
#include <cassert>


SFile::IdType SFile::IdCounter = 0;

SFile::SFile(std::uint32_t size, std::uint64_t expiresAt)
    : mId(++IdCounter),
      mSize(size),
      mExpiresAt(expiresAt)
{}



SReplica::SReplica(SFile* file, CStorageElement* storageElement, std::size_t indexAtStorageElement)
    : mFile(file),
      mStorageElement(storageElement),
      mIndexAtStorageElement(indexAtStorageElement)
{}
auto SReplica::Increase(std::uint32_t amount, std::uint64_t now) -> std::uint32_t
{
    mCurSize += amount;
    mStorageElement->OnIncreaseReplica(amount, now);
    return mCurSize;
}
void SReplica::Remove(std::uint64_t now)
{
    mStorageElement->OnRemoveReplica(*this, now);
}



CSite::CSite(const std::string& name)
    : mName(name)
{
    mStorageElements.reserve(16);
}

//auto CCSite::CreateLinkSelector(CSite& dstSite) -> CLinkSelector& {}

auto CSite::CreateStorageElement(const std::string& name) -> CStorageElement&
{
    mStorageElements.emplace_back(name, this);
    return mStorageElements.back();
}



CStorageElement::CStorageElement(const std::string& name, CSite* site)
    : mName(name),
      mSite(site)
{}

auto CStorageElement::CreateReplica(SFile& file) -> SReplica&
{
    const auto result = mFileIds.insert(file.GetId());
    assert(result.second==true);

    const std::size_t idx = mReplicas.size();
    mReplicas.emplace_back(&file, this, idx);
    return mReplicas.back();
}

void CStorageElement::OnIncreaseReplica(std::uint64_t amount, std::uint64_t now)
{
    mUsedStorage += amount;
}

void CStorageElement::OnRemoveReplica(const SReplica& replica, std::uint64_t now)
{
    const auto FileIdIterator = mFileIds.find(replica.GetFile()->GetId());
    const std::size_t idxToDelete = replica.mIndexAtStorageElement;
    const std::uint32_t curSize = replica.GetCurSize();

    assert(FileIdIterator != mFileIds.cend());
    assert(idxToDelete < mReplicas.size());
    assert(curSize <= mUsedStorage);

    SReplica& lastReplica = mReplicas.back();
    std::size_t& idxLastReplica = lastReplica.mIndexAtStorageElement;

    mFileIds.erase(FileIdIterator);
    mUsedStorage -= curSize;
    if(idxToDelete != idxLastReplica)
    {
        idxLastReplica = idxToDelete;
        mReplicas[idxToDelete] = std::move(lastReplica);
    }
    mReplicas.pop_back();
}



CRucio::CRucio()
{
    mFiles.reserve(65536);
}

auto CRucio::CreateFile(std::uint32_t size, std::uint64_t expiresAt) -> SFile&
{
    mFiles.emplace_back(size, expiresAt);
    return mFiles.back();
}

auto CRucio::RunReaper(std::uint64_t now) -> std::size_t
{
    const std::size_t numFiles = mFiles.size();

    if(numFiles == 0)
        return 0;

    std::size_t frontIdx = 0;
    std::size_t backIdx = numFiles - 1;

    while(backIdx > frontIdx && mFiles[backIdx].mExpiresAt <= now)
    {
        mFiles.pop_back();
        --backIdx;
    }

    while(frontIdx < backIdx)
    {
        if(mFiles[frontIdx].mExpiresAt <= now)
        {
            mFiles[frontIdx] = std::move(mFiles[backIdx]);
            do
            {
                mFiles.pop_back();
                --backIdx;
            } while(backIdx > frontIdx && mFiles[backIdx].mExpiresAt <= now);
        }
        ++frontIdx;
    }

    if(backIdx == 0)
    {
        if(mFiles.back().mExpiresAt <= now)
            mFiles.pop_back();
    }

    return numFiles - mFiles.size();
}
