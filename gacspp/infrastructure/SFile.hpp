#pragma once

#include <bitset>
#include <memory>
#include <vector>

#include "common/constants.h"

class CStorageElement;
struct SReplica;


struct SFile
{
    SFile(const SpaceType size, const TickType createdAt, const TickType lifetime, std::size_t indexAtRucio);

    void Remove(const TickType now);
    void RemoveReplica(const TickType now, const std::shared_ptr<SReplica>& replica);
    auto RemoveExpiredReplicas(const TickType now) -> std::size_t;
    auto ExtractExpiredReplicas(const TickType now, std::vector<std::shared_ptr<SReplica>>& expiredReplicas) -> std::size_t;

    void ExtendExpirationTime(const TickType newExpiresAt);

    auto GetReplicaByStorageElement(const CStorageElement* storageElement) -> std::shared_ptr<SReplica>;

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetCreatedAt() const -> TickType
    {return mCreatedAt;}
    inline auto GetSize() const -> SpaceType
    {return mSize;}

    std::vector<std::shared_ptr<SReplica>> mReplicas;
    TickType mExpiresAt;

    std::uint32_t mPopularity = 1; //workaroung: to be removed
   // std::size_t mIndexAtRucio; //another workaround

    //constexpr static std::size_t FLAG_IS_DELETED = 0;
    //std::bitset<8> mFlags;
private:
    IdType mId;
    TickType mCreatedAt;
    SpaceType mSize;
};

struct SReplica
{
    SReplica(std::shared_ptr<SFile>& file, CStorageElement* const storageElement, const TickType createdAt, const std::size_t indexAtStorageElement);

    auto Increase(const SpaceType amount, const TickType now) -> SpaceType;
    void OnRemoveByFile(const TickType now);

    void ExtendExpirationTime(const TickType newExpiresAt);

    inline bool IsComplete() const
    {return mCurSize == mFile->GetSize();}

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetCreatedAt() const -> TickType
    {return mCreatedAt;}
    inline auto GetFile() -> std::shared_ptr<SFile>
    {return mFile;}
    inline auto GetFile() const -> const std::shared_ptr<SFile>&
    {return mFile;}
    inline auto GetStorageElement() -> CStorageElement*
    {return mStorageElement;}
    inline auto GetCurSize() const -> SpaceType
    {return mCurSize;}


    std::size_t mIndexAtStorageElement;
    TickType mExpiresAt;

    std::uint32_t mNumStagedIn = 0; //workaroung: to be removed
    
    //constexpr static std::size_t FLAG_IS_DELETED = 0;
    //std::bitset<8> mFlags;
private:
    IdType mId;
    TickType mCreatedAt;
    std::shared_ptr<SFile> mFile;
    CStorageElement* mStorageElement;
    SpaceType mCurSize = 0;
};
