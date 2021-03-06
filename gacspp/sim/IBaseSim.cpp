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
    while(mIsRunning && (mCurrentTick <= maxTick) && !mSchedule.empty())
    {
        std::shared_ptr<CScheduleable> element = mSchedule.top();
        mSchedule.pop();

        assert(mCurrentTick <= element->mNextCallTick);

        mCurrentTick = element->mNextCallTick;
        element->OnUpdate(mCurrentTick);
        if(element->mNextCallTick > mCurrentTick)
            mSchedule.push(element);
        else
            element->Shutdown(mCurrentTick);
    }

    mIsRunning = false;

    while(!mSchedule.empty())
    {
        mSchedule.top()->Shutdown(mCurrentTick);
        mSchedule.pop();
    }

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
