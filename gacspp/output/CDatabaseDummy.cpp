#include "CDatabaseDummy.hpp"


namespace dummydb
{

void CInsertValuesContainer::AddValue(double value)
{
    (void)value;
}

void CInsertValuesContainer::AddValue(int value)
{
    (void)value;
}

void CInsertValuesContainer::AddValue(std::uint32_t value)
{
    (void)value;
}

void CInsertValuesContainer::AddValue(std::uint64_t value)
{
    (void)value;
}

void CInsertValuesContainer::AddValue(const std::string& value)
{
    (void)value;
}

void CInsertValuesContainer::AddValue(std::string&& value)
{
    (void)value;
}

bool CInsertValuesContainer::MergeIfPossible(std::unique_ptr<IInsertValuesContainer>& other)
{
    (void)other;
    return true;
}

auto CInsertValuesContainer::InsertValues() -> std::size_t
{
    return 0;
}


auto CPreparedInsert::CreateValuesContainer(std::size_t numReserveValues) -> std::unique_ptr<IInsertValuesContainer>
{
    (void)numReserveValues;
    return std::make_unique<CInsertValuesContainer>(mDB);
}



bool CDatabase::Open(const std::string& params)
{
    (void)params;
    return true;
}

bool CDatabase::Close()
{
    return true;
}


bool CDatabase::ExecuteQuery(const std::string& query)
{
    (void)query;
    return true;
}

auto CDatabase::PrepareInsert(const std::shared_ptr<IDatabase>& db, const std::string& queryTpl, std::size_t numWildcards, char wildcard) -> std::shared_ptr<IPreparedInsert>
{
    (void)queryTpl;
    (void)numWildcards;
    (void)wildcard;
    return std::make_shared<CPreparedInsert>(db, 0);
}

bool CDatabase::BeginTransaction()
{
    return true;
}

bool CDatabase::CommitAndBeginTransaction()
{
    return true;
}

bool CDatabase::EndTransaction()
{
    return true;
}

}
