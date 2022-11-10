#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/MergeTreeBaseSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/SelectQueryInfo.h>
#include "Storages/StorageSnapshot.h"

#include <mutex>
#include <memory>

namespace DB
{


class MergeTreeReadPoolParallelReplicas :  private boost::noncopyable
{

public:

    MergeTreeReadPoolParallelReplicas(
        StorageSnapshotPtr storage_snapshot_,
        size_t threads_,
        ParallelReadingExtension extension_,
        RangesInDataParts && parts_,
        size_t min_marks_for_concurrent_read_
    )
    : storage_snapshot(storage_snapshot_)
    , extension(extension_)
    , parts_ranges(parts_)
    , threads(threads_)
    , min_marks_for_concurrent_read(min_marks_for_concurrent_read_)
    {}

    /// Sends all the data about selected parts to the initiator
    void initialize();

    static MergeTreeReadTaskPtr getTask(size_t thread);

    Block getHeader();

    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) {}

private:
    StorageSnapshotPtr storage_snapshot;
    ParallelReadingExtension extension;
    RangesInDataParts parts_ranges;
    [[maybe_unused]] size_t threads;
    [[maybe_unused]] size_t min_marks_for_concurrent_read;

    mutable std::mutex mutex;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeReadPoolParallelReplicas");

};

using MergeTreeReadPoolParallelReplicasPtr = std::shared_ptr<MergeTreeReadPoolParallelReplicas>;

}
