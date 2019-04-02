#include <cassert>

#include "IBaseSim.hpp"

void IBaseSim::Run(const TickType maxTick)
{
    mCurrentTick = 0;
    while(mCurrentTick<=maxTick && !mSchedule.empty())
    {
        std::shared_ptr<CScheduleable> element = mSchedule.top();
        mSchedule.pop();

        assert(mCurrentTick <= element->mNextCallTick);

        mCurrentTick = element->mNextCallTick;
        element->OnUpdate(mCurrentTick);
        if(element->mNextCallTick > mCurrentTick)
            mSchedule.push(element);
    }
}
