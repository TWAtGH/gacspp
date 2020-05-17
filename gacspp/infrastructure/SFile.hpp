#pragma once

#include <memory>
#include <vector>

#include "common/constants.h"

class CStorageElement;
class IReplicaPreRemoveListener;
struct SReplica;


struct SFile
{
    SFile(SpaceType size, TickType createdAt, TickType lifetime, std::size_t indexAtRucio);

    void PostCreateReplica(SReplica* replica);
    void PreRemoveReplica(const SReplica* replica);

    void ExtendExpirationTime(TickType newExpiresAt);

    auto GetReplicaByStorageElement(const CStorageElement* storageElement) const -> SReplica*;

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetCreatedAt() const -> TickType
    {return mCreatedAt;}
    inline auto GetSize() const -> SpaceType
    {return mSize;}
    inline auto GetReplicas() const -> const std::vector<SReplica*>&
    {return mReplicas;}


    std::size_t mIndexAtRucio;

    TickType mExpiresAt;

    std::uint32_t mPopularity = 1; //workaround: to be removed

private:
    IdType mId;
    TickType mCreatedAt;
    SpaceType mSize;

    std::vector<SReplica*> mReplicas;
};

struct SReplica
{
    SReplica(SFile* file, CStorageElement* storageElement, TickType createdAt, std::size_t indexAtStorageElement);

    auto Increase(SpaceType amount, TickType now) -> SpaceType;

    void ExtendExpirationTime(TickType newExpiresAt);

    inline bool IsComplete() const
    {return mCurSize == mFile->GetSize();}

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetCreatedAt() const -> TickType
    {return mCreatedAt;}
    inline auto GetCurSize() const -> SpaceType
    {return mCurSize;}

    inline auto GetFile() const -> SFile*
    {return mFile;}
    inline auto GetStorageElement() const -> CStorageElement*
    {return mStorageElement;}

public:
    IReplicaPreRemoveListener* mRemoveListener = nullptr;

    std::size_t mIndexAtStorageElement;
    TickType mExpiresAt;

    std::uint32_t mUsageCounter = 0;

private:
    IdType mId;
    TickType mCreatedAt;
    SFile* mFile;
    CStorageElement* mStorageElement;
    SpaceType mCurSize = 0;
};


struct SIndexedReplicas
{
private:
    std::unordered_map<SReplica*, std::size_t> mReplicaToIdx;
    std::vector<SReplica*> mReplicas;

public:
    inline auto IsEmpty() const -> std::size_t
    {return mReplicas.empty();}

    inline auto NumReplicas() const -> std::size_t
    {return mReplicas.size();}
    
    inline auto GetReplica(std::size_t idx) const -> SReplica*
    {return mReplicas[idx];}
    
    inline bool HasReplica(SReplica* replica) const
    {return (mReplicaToIdx.count(replica) > 0);}

    bool AddReplica(SReplica* replica);
    bool RemoveReplica(SReplica* replica);
    bool RemoveReplica(std::size_t idx);
    auto ExtractBack() -> SReplica*;
};