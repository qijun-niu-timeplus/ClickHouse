#include <mutex>
#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicas.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include "IO/WriteBuffer.h"
#include "IO/WriteBufferFromString.h"
#include "Interpreters/Context.h"

namespace DB
{


void MergeTreeReadPoolParallelReplicas::initialize()
{
    std::cout << "MergeTreeReadPoolParallelReplicas::initialize()" << std::endl;

    std::cout << "parts_ranges.getMarksCountAllParts() " << parts_ranges.getMarksCountAllParts() << std::endl;

    WriteBufferFromOwnString result;
    auto desc = parts_ranges.getDescriptions();
    desc.describe(result);
    std::cout << result.str() << std::endl;

    extension.all_callback(
        InitialAllRangesAnnouncement{
            .description = desc,
            .replica_num = extension.number_of_current_replica,
        }
    );
}

Block MergeTreeReadPoolParallelReplicas::getHeader()
{
    return storage_snapshot->getSampleBlockForColumns(extension.colums_to_read);
}


std::vector<size_t> MergeTreeReadPoolParallelReplicas::fillPerPartInfo(const RangesInDataParts & parts)
{
    std::vector<size_t> per_part_sum_marks;
    Block sample_block = storage_snapshot->metadata->getSampleBlock();

    for (const auto i : collections::range(0, parts.size()))
    {
        const auto & part = parts[i];

        /// Read marks for every data part.
        size_t sum_marks = 0;
        for (const auto & range : part.ranges)
            sum_marks += range.end - range.begin;

        per_part_sum_marks.push_back(sum_marks);

        auto task_columns = getReadTaskColumns(
            LoadedMergeTreeDataPartInfoForReader(part.data_part), storage_snapshot,
            column_names, virtual_column_names, prewhere_info, /*with_subcolumns=*/ true);

        auto size_predictor = nullptr;

        auto & per_part = per_part_params.emplace_back();

        per_part.size_predictor = std::move(size_predictor);

        /// will be used to distinguish between PREWHERE and WHERE columns when applying filter
        const auto & required_column_names = task_columns.columns.getNames();
        per_part.column_name_set = {required_column_names.begin(), required_column_names.end()};
        per_part.task_columns = std::move(task_columns);

        parts_with_idx.push_back({ part.data_part, part.part_index_in_query });
    }

    return per_part_sum_marks;
}



MergeTreeReadTaskPtr MergeTreeReadPoolParallelReplicas::getTask()
{
    std::lock_guard lock(mutex);

    std::cout << "Requested a task" << std::endl;

    if (buffered_ranges.empty())
    {
        std::cout << "Going to collaborate with initiator" << std::endl;

        auto result = extension.callback(ParallelReadRequest{
            .replica_num = extension.number_of_current_replica,
            .min_number_of_marks = min_marks_for_concurrent_read * threads * 2
        });

        if (!result || result->finish)
        {
            return nullptr;
        }


        WriteBufferFromOwnString ss;
        result->describe(ss);
        std::cout << "Got answer " << ss.str() << std::endl;

        buffered_ranges = std::move(result->description);
    }

    auto & current_task = buffered_ranges.front();

    RangesInDataPart part;
    size_t part_idx = 0;
    for (auto & other_part : parts_ranges)
    {
        if (other_part.data_part->info == current_task.info)
        {
            part = other_part;
            part_idx = other_part.part_index_in_query;
            break;
        }
    }

    MarkRanges ranges_to_read;
    size_t current_sum_marks = 0;
    while (current_sum_marks < min_marks_for_concurrent_read && !current_task.ranges.empty())
    {
        auto diff = min_marks_for_concurrent_read - current_sum_marks;
        auto range = current_task.ranges.front();
        if (range.getNumberOfMarks() > diff)
        {
            auto new_range = range;
            new_range.end = range.begin + diff;
            range.begin += diff;

            current_task.ranges.front() = range;
            ranges_to_read.push_back(new_range);
            current_sum_marks += new_range.getNumberOfMarks();
            continue;
        }

        ranges_to_read.push_back(range);
        current_sum_marks += range.getNumberOfMarks();
        current_task.ranges.pop_front();
    }

    if (current_task.ranges.empty())
        buffered_ranges.pop_front();

    const auto & per_part = per_part_params[part_idx];

    auto curr_task_size_predictor = !per_part.size_predictor ? nullptr
        : std::make_unique<MergeTreeBlockSizePredictor>(*per_part.size_predictor); /// make a copy

    std::cout << "Will read from part {}" << part.data_part->info.getPartName() << std::endl;
    for (auto & range : ranges_to_read)
    {
        std::cout << range.begin << ' ' << range.end << std::endl;
    }


    return std::make_unique<MergeTreeReadTask>(
        part.data_part, ranges_to_read, part.part_index_in_query, column_names,
        per_part.column_name_set, per_part.task_columns,
        prewhere_info && prewhere_info->remove_prewhere_column, std::move(curr_task_size_predictor));
}

}
