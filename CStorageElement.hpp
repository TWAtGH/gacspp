#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_set>
#include <vector>

#include "constants.h"
#include "parallel_hashmap/phmap.h"

class ISite;
struct SFile;
struct SReplica;

class IPreparedInsert;


class CStorageElement
{
public:
    enum OPERATION
    {
        INSERT,
        GET,
        CREATE_TRANSFER,
        DELETE,
        CUSTOM
    };

    static std::shared_ptr<IPreparedInsert> outputReplicaInsertQuery;

	CStorageElement(std::string&& name, const TickType accessLatency, ISite* const site);
    CStorageElement(CStorageElement&&) = default;
    CStorageElement& operator=(CStorageElement&&) = default;

    CStorageElement(CStorageElement const&) = delete;
    CStorageElement& operator=(CStorageElement const&) = delete;

    virtual ~CStorageElement();


    virtual void OnOperation(const OPERATION op);

	virtual auto CreateReplica(std::shared_ptr<SFile>& file) -> std::shared_ptr<SReplica>;
    virtual void OnIncreaseReplica(const std::uint64_t amount, const TickType now);
    virtual void OnRemoveReplica(const SReplica* replica, const TickType now, bool needLock=true);

	inline auto GetId() const -> IdType
	{return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetAccessLatency() const -> TickType
    {return mAccessLatency;}
    inline auto GetSite() const -> const ISite*
    {return mSite;}
    inline auto GetSite() -> ISite*
    {return mSite;}

private:
    IdType mId;
    std::string mName;
    TickType mAccessLatency;
    phmap::parallel_flat_hash_set<IdType> mFileIds;

protected:
    std::mutex mReplicaRemoveMutex;

    ISite* mSite;
    std::uint64_t mUsedStorage = 0;

public:
	std::vector<std::shared_ptr<SReplica>> mReplicas;

};
