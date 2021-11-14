#include <cassert>
#include <iostream>

#include "IBaseSim.hpp"

#include "clouds/IBaseCloud.hpp"

#include "infrastructure/CRucio.hpp"
#include "infrastructure/CStorageElement.hpp"

IBaseSim* IBaseSim::Sim = nullptr;

IBaseSim::IBaseSim()
{
    std::random_device rd;
    auto seed = rd();
    std::cout<<"Using seed: "<<seed<<std::endl;
    mRNGEngine = RNGEngineType(seed);
    IBaseSim::Sim = this;
}
IBaseSim::~IBaseSim() = default;

void IBaseSim::Run(TickType maxTick)
{
    mCurrentTick = 0;
    mIsRunning = true;
    ScheduleType::node_type curNode;
    std::shared_ptr<CSchedulable> curEvent;
    while (mIsRunning && (mCurrentTick <= maxTick) && !mSchedule.empty())
    {
        curNode = mSchedule.extract(mSchedule.begin());
        curEvent = curNode.value();

        assert(mCurrentTick <= curEvent->mNextCallTick);

        mCurrentTick = curEvent->mNextCallTick;
        curEvent->OnUpdate(mCurrentTick);

        if (curEvent->mNextCallTick > mCurrentTick)
            mSchedule.insert(std::move(curNode));
        else
            curEvent->Shutdown(mCurrentTick);
    }

    mIsRunning = false;

    for (auto& element : mSchedule)
        element->Shutdown(mCurrentTick);
    mSchedule.clear();

    mRucio->RemoveAllFiles(mCurrentTick);
}

void IBaseSim::Stop()
{
    mIsRunning = false;
}

auto IBaseSim::GetStorageElementByName(const std::string& name) const -> CStorageElement*
{
    CStorageElement* storageElement = mRucio->GetStorageElementByName(name);
    if (storageElement)
        return storageElement;

    for (const std::unique_ptr<IBaseCloud>& cloud : mClouds)
    {
        storageElement = cloud->GetStorageElementByName(name);
        if (storageElement)
            return storageElement;
    }
    return nullptr;
}
