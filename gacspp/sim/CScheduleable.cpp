#include "CScheduleable.hpp"

bool SSchedulePrioComparer::operator()(const CScheduleable *left, const CScheduleable *right) const
{
    return left->mNextCallTick > right->mNextCallTick;
}

bool SSchedulePrioComparer::operator()(const std::shared_ptr<CScheduleable>& left, const std::shared_ptr<CScheduleable>& right) const
{
    return left->mNextCallTick > right->mNextCallTick;
}
