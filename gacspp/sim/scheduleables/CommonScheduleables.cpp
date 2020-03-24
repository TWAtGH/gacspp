#include <algorithm>
#include <cassert>
#include <iomanip>
#include <iostream>
#include <limits>
#include <sstream>

#include "CommonScheduleables.hpp"
#include "TransferManager.hpp"

#include "sim/IBaseSim.hpp"

#include "clouds/IBaseCloud.hpp"

#include "infrastructure/CRucio.hpp"
#include "infrastructure/CStorageElement.hpp"
#include "infrastructure/SFile.hpp"

#include "output/COutput.hpp"


CDataGenerator::CDataGenerator( IBaseSim* sim,
                                std::unique_ptr<IValueGenerator>&& numFilesGen,
                                std::unique_ptr<IValueGenerator>&& fileSizeGen,
                                std::unique_ptr<IValueGenerator>&& fileLifetimeGen,
                                const TickType tickFreq,
                                const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mNumFilesGen(std::move(numFilesGen)),
      mFileSizeGen(std::move(fileSizeGen)),
      mFileLifetimeGen(std::move(fileLifetimeGen)),
      mTickFreq(tickFreq)
{
    assert(mNumFilesGen);
    assert(mFileSizeGen);
    assert(mFileLifetimeGen);
}

void  CDataGenerator::CreateFilesAndReplicas(const std::uint32_t numFiles, const std::uint32_t numReplicasPerFile, const TickType now)
{
    if(numFiles == 0 || numReplicasPerFile == 0)
        return;

    const std::uint32_t numStorageElements = static_cast<std::uint32_t>( mStorageElements.size() );

    assert(numReplicasPerFile <= numStorageElements);

    std::uniform_int_distribution<std::uint32_t> rngSampler(0, numStorageElements);
    for(std::uint32_t i = 0; i < numFiles; ++i)
    {
        const SpaceType fileSize = GiB_TO_BYTES(static_cast<SpaceType>(mFileSizeGen->GetValue(mSim->mRNGEngine)));
        const TickType lifetime = DAYS_TO_SECONDS(static_cast<TickType>(mFileLifetimeGen->GetValue(mSim->mRNGEngine)));

        std::shared_ptr<SFile> file = mSim->mRucio->CreateFile(fileSize, now, lifetime);

        auto reverseRSEIt = mStorageElements.rbegin();
        auto selectedElementIt = mStorageElements.begin();
        //numReplicasPerFile <= numStorageElements !
        for(std::uint32_t numCreated = 0; numCreated<numReplicasPerFile; ++numCreated)
        {
            if(mSelectStorageElementsRandomly)
                selectedElementIt = mStorageElements.begin() + (rngSampler(mSim->mRNGEngine) % (numStorageElements - numCreated));

            std::shared_ptr<SReplica> r = (*selectedElementIt)->CreateReplica(file, now);

            r->Increase(fileSize, now);
            r->mExpiresAt = now + (lifetime / numReplicasPerFile);

            if(mSelectStorageElementsRandomly)
            {
                std::iter_swap(selectedElementIt, reverseRSEIt);
                ++reverseRSEIt;
            }
            else
                ++selectedElementIt;
        }
    }
}

void CDataGenerator::CreateFilesAndReplicas(const TickType now)
{
    const std::uint32_t totalFilesToGen = mNumFilesGen->GetValue(mSim->mRNGEngine);

    float numReplicaRatio = mNumReplicaRatio.empty() ? 1 : mNumReplicaRatio[0];
    std::uint32_t numFilesToGen = static_cast<std::uint32_t>(totalFilesToGen * numReplicaRatio);
    CreateFilesAndReplicas(numFilesToGen, 1, now);

    for(std::size_t i=1; i<mNumReplicaRatio.size(); ++i)
    {
        numFilesToGen = static_cast<std::uint32_t>(totalFilesToGen * mNumReplicaRatio[i]);
        CreateFilesAndReplicas(numFilesToGen, i+1, now);
    }
}

void CDataGenerator::OnUpdate(const TickType now)
 {
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    CreateFilesAndReplicas(now);

    mNextCallTick = now + mTickFreq;
}



CReaperCaller::CReaperCaller(CRucio *rucio, const TickType tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mRucio(rucio),
      mTickFreq(tickFreq)
{}

void CReaperCaller::OnUpdate(const TickType now)
{
    CScopedTimeDiff durationUpdate(mUpdateDurationSummed, true);

    mRucio->RunReaper(now);

    mNextCallTick = now + mTickFreq;
}



CBillingGenerator::CBillingGenerator(IBaseSim* sim, const TickType tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTickFreq(tickFreq)
{
    mCloudBillInsertQuery = COutput::GetRef().CreatePreparedInsert("COPY Bills(cloudName, month, bill) FROM STDIN with(FORMAT csv);", 3, '?');
}

void CBillingGenerator::OnUpdate(const TickType now)
{
    std::stringstream summary;

    std::unique_ptr<IInsertValuesContainer> billInsertQueries = mCloudBillInsertQuery->CreateValuesContainer(3 * mSim->mClouds.size());

    const std::uint32_t month = static_cast<std::uint32_t>(SECONDS_TO_MONTHS(now));

    const std::string caption = std::string(10, '=') + " Monthly Summary " + std::string(10, '=');

    summary << std::fixed << std::setprecision(2);

    summary << std::endl;
    summary << std::string(caption.length(), '=') << std::endl;
    summary << caption << std::endl;
    summary << std::string(caption.length(), '=') << std::endl;

    for(const std::unique_ptr<IBaseCloud>& cloud : mSim->mClouds)
    {
        std::string bill = cloud->ProcessBilling(now)->ToString();
        summary << std::endl;
        summary<<cloud->GetName()<<" - Billing for Month "<< month <<":\n";
        summary << bill;
        billInsertQueries->AddValue(cloud->GetName());
        billInsertQueries->AddValue(month);
        billInsertQueries->AddValue(std::move(bill));
    }

    summary << std::string(caption.length(), '=') << std::endl;
    std::cout << summary.str() << std::endl;

    COutput::GetRef().QueueInserts(std::move(billInsertQueries));

    mNextCallTick = now + mTickFreq;
}



CHeartbeat::CHeartbeat(IBaseSim* sim, const TickType tickFreq, const TickType startTick)
    : CScheduleable(startTick),
      mSim(sim),
      mTickFreq(tickFreq)
{
    mTimeLastUpdate = std::chrono::high_resolution_clock::now();
}

void CHeartbeat::OnUpdate(const TickType now)
{
    auto curRealtime = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double> timeDiff = curRealtime - mTimeLastUpdate;
    mUpdateDurationSummed += timeDiff;
    mTimeLastUpdate = curRealtime;

    std::stringstream statusOutput;
    statusOutput << std::fixed << std::setprecision(2);

    statusOutput << "[" << std::setw(6) << static_cast<TickType>(now / 1000.0) << "k]: ";
    statusOutput << "Runtime: " << mUpdateDurationSummed.count() << "s; ";
    statusOutput << "numFiles: " << static_cast<std::size_t>(mSim->mRucio->mFiles.size() / 1000.0) << "k" << std::endl;

    statusOutput << "Transfer stats:" << std::endl;
    for (std::shared_ptr<CBaseTransferManager>& transferManager : mTransferManagers)
    {
        std::uint64_t sumNumTransfers = transferManager->GetNumActiveTransfers() + transferManager->mNumCompletedTransfers + transferManager->mNumFailedTransfers;
        statusOutput << transferManager->mName << std::endl;

        statusOutput << "  avg duration: ";
        if (sumNumTransfers > 0)
            statusOutput << transferManager->mSummedTransferDuration / sumNumTransfers;
        else
            statusOutput << "-";
        statusOutput << std::endl;

        statusOutput << "        active: " << transferManager->GetNumActiveTransfers() << std::endl;
        statusOutput << "          done: " << transferManager->mNumCompletedTransfers << std::endl;
        statusOutput << "        failed: " << transferManager->mNumFailedTransfers << std::endl;

        transferManager->mSummedTransferDuration = 0;
        transferManager->mNumCompletedTransfers = 0;
        transferManager->mNumFailedTransfers = 0;
    }


    std::size_t maxW = 0;
    for (auto it : mProccessDurations)
        if (it.first.size() > maxW)
            maxW = it.first.size();

    statusOutput << "Sim stats:" << std::endl;
    statusOutput << "  " << std::setw(maxW) << "Duration: " << std::setw(6) << timeDiff.count() << "s\n";
    for(auto it : mProccessDurations)
    {
        statusOutput << "  " << std::setw(maxW) << it.first;
        statusOutput << ": " << std::setw(6) << it.second->count();
        statusOutput << "s ("<< std::setw(5) << (it.second->count() / timeDiff.count()) * 100 << "%)\n";
        *(it.second) = std::chrono::duration<double>::zero();
    }
    std::cout << statusOutput.str() << std::endl;

    mNextCallTick = now + mTickFreq;
}
