#pragma once

#include <memory>

#include "common/constants.h"

struct SFile;
struct SReplica;
class CStorageElement;


class IFileActionListener
{
public:
    virtual void OnFileCreated(const TickType now, std::shared_ptr<SFile> file) = 0;
    virtual void OnFilesDeleted(const TickType now, const std::vector<std::weak_ptr<SFile>>& deletedFiles) = 0;
};



class IReplicaActionListener
{
public:
    virtual void OnReplicaCreated(const TickType now, std::shared_ptr<SReplica> replica) = 0;
    virtual void OnReplicaDeleted(const TickType now, std::weak_ptr<SReplica> replica) = 0;
};
