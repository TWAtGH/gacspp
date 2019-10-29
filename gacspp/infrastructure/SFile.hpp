#pragma once

#include <memory>
#include <vector>

#include "common/constants.h"

class CStorageElement;
struct SReplica;


struct SFile
{
    SFile(const SpaceType size, const TickType createdAt, const TickType lifetime);
    SFile(SFile&&) = default;
    SFile& operator=(SFile&&) = default;

    SFile(SFile const&) = delete;
    SFile& operator=(SFile const&) = delete;

    void Remove(const TickType now);
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

private:
    IdType mId;
    TickType mCreatedAt;
    SpaceType mSize;
};

struct SReplica
{
    SReplica(std::shared_ptr<SFile>& file, CStorageElement* const storageElement, const TickType createdAt, const std::size_t indexAtStorageElement);

    SReplica(SReplica&&) = default;
    SReplica& operator=(SReplica&&) = default;

    SReplica(SReplica const&) = delete;
    SReplica& operator=(SReplica const&) = delete;

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

private:
    IdType mId;
    TickType mCreatedAt;
    std::shared_ptr<SFile> mFile;
    CStorageElement* mStorageElement;
    SpaceType mCurSize = 0;
};
