#include <Storages/MergeTree/ParallelReplicasReadingCoordinator.h>

#include <algorithm>
#include <mutex>
#include <ostream>
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
#include "Storages/MergeTree/RangesInDataPart.h"
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

    explicit Impl(size_t replicas_count_)
        : replicas_count(replicas_count_)
        , announcements(replicas_count_)
    {}

    struct PartitionReading
    {
        PartSegments part_ranges;
        PartToMarkRanges mark_ranges_in_part;
    };

    using PartitionToBlockRanges = std::map<String, PartitionReading>;
    PartitionToBlockRanges partitions;

    std::mutex mutex;
    size_t replicas_count{0};
    size_t sent_initial_requests{0};
    std::vector<InitialAllRangesAnnouncement> announcements;

    struct Part
    {
        RangesInDataPartDescription description;
        std::vector<size_t> replicas;
    };

    std::vector<Part> reading_state;

    ParallelReadResponse handleRequest(ParallelReadRequest request);
    void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement);

    void computeReadingState();
};

void ParallelReplicasReadingCoordinator::Impl::computeReadingState()
{
    for (auto & replica_parts: announcements)
    {
        for (auto & part: replica_parts.description)
        {
            bool have_exactly_this_part = false;
            size_t exact_part_index = 0;
            for (size_t i = 0; i < reading_state.size(); ++i)
            {
                if (reading_state[i].description.info == part.info)
                {
                    have_exactly_this_part = true;
                    exact_part_index = i;
                    break;
                }
            }
            if (have_exactly_this_part)
            {
                reading_state[exact_part_index].replicas.push_back(replica_parts.replica_num);
                continue;
            }

            reading_state.push_back({
                .description = part,
                .replicas = {replica_parts.replica_num}
            });
        }
    }

    std::cout << "Overall reading state" << std::endl;
    for (auto & part : reading_state)
    {
        WriteBufferFromOwnString ss;
        part.description.describe(ss);
        std::cout << ss.str();
        std::cout << "Found on " << fmt::format("{}", fmt::join(part.replicas, ", ")) << std::endl;
    }
}


void ParallelReplicasReadingCoordinator::Impl::handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
{
    std::lock_guard lock(mutex);

    WriteBufferFromOwnString ss;
    announcement.describe(ss);
    std::cout << "Received an announecement" << std::endl;
    std::cout << ss.str() << std::endl;

    announcements[announcement.replica_num] = std::move(announcement);

    ++sent_initial_requests;
    if (sent_initial_requests == replicas_count)
        computeReadingState();
}

ParallelReadResponse ParallelReplicasReadingCoordinator::Impl::handleRequest(ParallelReadRequest request)
{
    AtomicStopwatch watch;
    std::lock_guard lock(mutex);

    LOG_TRACE(&Poco::Logger::get("Anime"), "Handling request from replica {}, minimal marks size is {}", request.replica_num, request.min_number_of_marks );

    size_t current_mark_size = 0;
    ParallelReadResponse response;

    for (auto & part : reading_state)
    {
        if (std::find(part.replicas.begin(), part.replicas.end(), request.replica_num) == part.replicas.end())
            continue;

        if (current_mark_size >= request.min_number_of_marks)
            continue;

        if (part.description.ranges.empty())
            continue;

        response.description.push_back({
            .info = part.description.info,
            .ranges = {},
        });

        while (!part.description.ranges.empty() && current_mark_size < request.min_number_of_marks)
        {
            auto range = part.description.ranges.front();

            if (range.getNumberOfMarks() > request.min_number_of_marks)
            {
                auto new_range = range;
                range.begin += request.min_number_of_marks;
                new_range.end = new_range.begin + request.min_number_of_marks;

                part.description.ranges.front() = range;

                response.description.back().ranges.emplace_back(new_range);
                current_mark_size += new_range.getNumberOfMarks();
                continue;
            }

            current_mark_size += part.description.ranges.front().getNumberOfMarks();
            response.description.back().ranges.emplace_back(part.description.ranges.front());
            part.description.ranges.pop_front();
        }
    }

    if (response.description.empty())
        response.finish = true;

    WriteBufferFromOwnString ss;
    response.describe(ss);

    LOG_TRACE(&Poco::Logger::get("Anime"), "Going to respond to replica {} with {}", request.replica_num, ss.str());

    return response;
}

void ParallelReplicasReadingCoordinator::handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement announcement)
{
    return pimpl->handleInitialAllRangesAnnouncement(announcement);
}


ParallelReadResponse ParallelReplicasReadingCoordinator::handleRequest(ParallelReadRequest request)
{
    return pimpl->handleRequest(std::move(request));
}

ParallelReplicasReadingCoordinator::ParallelReplicasReadingCoordinator(size_t replicas_count_)
{
    pimpl = std::make_unique<ParallelReplicasReadingCoordinator::Impl>(replicas_count_);
}

ParallelReplicasReadingCoordinator::~ParallelReplicasReadingCoordinator() = default;

}
