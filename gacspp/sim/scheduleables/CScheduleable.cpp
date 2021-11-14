#include "CScheduleable.hpp"

bool SSchedulePrioComparer::operator()(const CSchedulable*left, const CSchedulable*right) const
{
    return left->mNextCallTick < right->mNextCallTick;
}

bool SSchedulePrioComparer::operator()(const std::shared_ptr<CSchedulable>& left, const std::shared_ptr<CSchedulable>& right) const
{
    return left->mNextCallTick < right->mNextCallTick;
}
