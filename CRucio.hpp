#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>



class CStorageElement;
struct SReplica;

class CLinkSelector
{
public:
	typedef std::vector<std::pair<std::uint64_t, double>> PriceInfoType;

	CLinkSelector(std::uint32_t bandwidth)
		: mBandwidth(bandwidth)
	{}
    PriceInfoType mNetworkPrice = {{0,0}};
    std::uint64_t mUsedTraffic = 0;
	std::uint32_t mNumActiveTransfers = 0;
	std::uint32_t mBandwidth;
};

struct SFile
{
public:
    typedef std::uint64_t IdType;

private:
    static IdType IdCounter;

    IdType mId;
    std::uint32_t mSize;

public:
	std::vector<SReplica*> mReplicas;
    std::unordered_set<CStorageElement*> mStorageElements;

    std::uint64_t mExpiresAt;

    SFile(std::uint32_t size, std::uint64_t expiresAt);
    SFile(SFile&&) = default;
    SFile& operator=(SFile&&) = default;

    SFile(SFile const&) = delete;
    SFile& operator=(SFile const&) = delete;

    inline auto GetId() const -> IdType
    {return mId;}
    inline auto GetSize() const -> std::uint32_t
    {return mSize;}
};

struct SReplica
{
private:
    SFile* mFile;
    CStorageElement* mStorageElement;
    std::uint32_t mCurSize = 0;

public:
    std::size_t mIndexAtStorageElement;

    SReplica(SFile* file, CStorageElement* storageElement, std::size_t indexAtStorageElement);
    SReplica(SReplica&&) = default;
    SReplica& operator=(SReplica&&) = default;

    SReplica(SReplica const&) = delete;
    SReplica& operator=(SReplica const&) = delete;

    auto Increase(std::uint32_t amount, std::uint64_t now) -> std::uint32_t;
    void Remove(std::uint64_t now);

	bool IsComplete() const
	{return mCurSize == mFile->GetSize();}

    inline auto GetFile() -> SFile*
    {return mFile;}
    inline auto GetFile() const -> const SFile*
    {return mFile;}
    inline auto GetStorageElement() -> CStorageElement*
    {return mStorageElement;}
    inline auto GetCurSize() const -> std::uint32_t
    {return mCurSize;}
};

class ISite
{
public:
    typedef std::uint64_t IdType;

private:
    static IdType IdCounter;
	IdType mId;
    std::string mName;
	std::string mLocationName;

protected:
    std::vector<std::unique_ptr<CLinkSelector>> mLinkSelectors;
	std::unordered_map<IdType, std::size_t> mDstSiteIdToLinkSelectorIdx;

public:
	ISite(std::string&& name, std::string&& locationName);
	virtual ~ISite() = default;

	ISite(ISite&&) = default;
	ISite& operator=(ISite&&) = default;

	ISite(ISite const&) = delete;
	ISite& operator=(ISite const&) = delete;

	inline bool operator==(const ISite& b) const
	{return mId == b.mId;}
	inline bool operator!=(const ISite& b) const
	{return mId != b.mId;}

	virtual auto CreateLinkSelector(const ISite* dstSite, std::uint32_t bandwidth) -> CLinkSelector*;
    virtual auto CreateStorageElement(std::string&& name) -> CStorageElement* = 0;

	auto GetLinkSelector(const ISite* dstSite) -> CLinkSelector*;

	inline auto GetId() const -> IdType
	{return mId;}
    inline auto GetName() const -> const std::string&
    {return mName;}
	inline auto GetLocationName() const -> const std::string&
	{return mLocationName;}
};

class CGridSite : public ISite
{
private:
	std::vector<std::unique_ptr<CStorageElement>> mStorageElements;

public:
	CGridSite(std::string&& name, std::string&& locationName);
	CGridSite(CGridSite&&) = default;
	CGridSite& operator=(CGridSite&&) = default;

	CGridSite(CGridSite const&) = delete;
	CGridSite& operator=(CGridSite const&) = delete;

	auto CreateStorageElement(std::string&& name) -> CStorageElement*;
};

class CStorageElement
{
private:
    std::string mName;
	std::unordered_set<SFile::IdType> mFileIds;

protected:
    ISite* mSite;
    std::uint64_t mUsedStorage = 0;

public:
	std::vector<std::unique_ptr<SReplica>> mReplicas;

	CStorageElement(std::string&& name, ISite* site);
    CStorageElement(CStorageElement&&) = default;
    CStorageElement& operator=(CStorageElement&&) = default;

    CStorageElement(CStorageElement const&) = delete;
    CStorageElement& operator=(CStorageElement const&) = delete;

	auto CreateReplica(SFile* file) -> SReplica*;

    virtual void OnIncreaseReplica(std::uint64_t amount, std::uint64_t now);
    virtual void OnRemoveReplica(const SReplica* replica, std::uint64_t now);

    inline auto GetName() const -> const std::string&
    {return mName;}
    inline auto GetSite() const -> const ISite*
    {return mSite;}
    inline auto GetSite() -> ISite*
    {return mSite;}
};

class CRucio
{
public:
    std::vector<std::unique_ptr<SFile>> mFiles;
    std::vector<std::unique_ptr<CGridSite>> mGridSites;

    CRucio();
    auto CreateFile(std::uint32_t size, std::uint64_t expiresAt) -> SFile*;
    auto CreateGridSite(std::string&& name, std::string&& locationName) -> CGridSite*;
    auto RunReaper(std::uint64_t now) -> std::size_t;
};
