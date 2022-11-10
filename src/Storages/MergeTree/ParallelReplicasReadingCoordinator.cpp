#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

#include <algorithm>
#include <vector>
#include <compare>
#include <numeric>
#include <unordered_map>
#include <map>
#include <iostream>
#include <set>
#include <cassert>


#include <Common/logger_useful.h>
#include <base/types.h>
#include <base/scope_guard.h>
#include <Common/Stopwatch.h>
#include "IO/WriteBufferFromString.h"
#include "Storages/MergeTree/RequestResponse.h"
#include <Storages/MergeTree/MarkRange.h>
#include <Storages/MergeTree/IntersectionsIndexes.h>

namespace DB
{

class ParallelReplicasReadingCoordinator::Impl
{
public:
    using ParallelReadRequestPtr = std::unique_ptr<ParallelReadRequest>;
    using PartToMarkRanges = std::map<PartToRead::PartAndProjectionNames, HalfIntervals>;

    struct PartitionReading
    {
        PartSegments part_ranges;
        PartToMarkRanges mark_ranges_in_part;
    };

    using PartitionToBlockRanges = std::map<String, PartitionReading>;
    PartitionToBlockRanges partitions;

    std::mutex mutex;

    ParallelReadResponse handleRequest(ParallelReadRequest request);
    void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement);
};


void ParallelReplicasReadingCoordinator::Impl::handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
{
    WriteBufferFromOwnString ss;
    announcement.describe(ss);
    std::cout << "Received an announecement" << std::endl;
    std::cout << ss.str() << std::endl;
}

void ParallelReplicasReadingCoordinator::handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
{
    return pimpl->handleInitialAllRangesAnnouncement(announcement);
}

ParallelReadResponse ParallelReplicasReadingCoordinator::Impl::handleRequest(ParallelReadRequest request)
{
    AtomicStopwatch watch;
    std::lock_guard lock(mutex);

    (void)request;

    return ParallelReadResponse{};
}

ParallelReadResponse ParallelReplicasReadingCoordinator::handleRequest(ParallelReadRequest request)
{
    return pimpl->handleRequest(std::move(request));
}

ParallelReplicasReadingCoordinator::ParallelReplicasReadingCoordinator()
{
    pimpl = std::make_unique<ParallelReplicasReadingCoordinator::Impl>();
}

ParallelReplicasReadingCoordinator::~ParallelReplicasReadingCoordinator() = default;

}
