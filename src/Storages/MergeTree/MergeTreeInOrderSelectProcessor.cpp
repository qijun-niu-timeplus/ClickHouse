#include <Storages/MergeTree/MergeTreeInOrderSelectProcessor.h>
#include "Storages/MergeTree/RangesInDataPart.h"
#include <Storages/MergeTree/IntersectionsIndexes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

bool MergeTreeInOrderSelectProcessor::getNewTaskImpl()
try
{
    if (all_mark_ranges.empty())
        return false;

    if (!reader)
        initializeReaders();

    MarkRanges mark_ranges_for_task;

    if (!pool)
    {
        /// If we need to read few rows, set one range per task to reduce number of read data.
        if (has_limit_below_one_block)
        {
            mark_ranges_for_task = MarkRanges{};
            mark_ranges_for_task.emplace_front(std::move(all_mark_ranges.front()));
            all_mark_ranges.pop_front();
        }
        else
        {
            mark_ranges_for_task = std::move(all_mark_ranges);
            all_mark_ranges.clear();
        }
    }

    if (pool)
    {
        size_t current_sum_marks = 0;
        size_t max_marks = 100;

        while (!all_mark_ranges.empty() && current_sum_marks < max_marks)
        {
            auto current_range = all_mark_ranges.front();
            all_mark_ranges.pop_front();
            auto gap = max_marks - current_sum_marks;
            if (current_range.getNumberOfMarks() <= gap)
            {
                mark_ranges_for_task.emplace_back(current_range);
                current_sum_marks += current_range.getNumberOfMarks();
                continue;
            }

            auto new_range = current_range;
            new_range.end = new_range.begin + gap;
            current_range.begin += gap;
            current_sum_marks += gap;
            mark_ranges_for_task.emplace_back(new_range);
            all_mark_ranges.emplace_front(current_range);
        }

        auto description = RangesInDataPartDescription{
            .info = data_part->info,
            .ranges = mark_ranges_for_task,
        };

        mark_ranges_for_task = pool->getNewTask(description);

        if (mark_ranges_for_task.empty())
            return false;

        /// We need to subtract new ranges from all ranges we have now
        auto current_ranges = HalfIntervals::initializeFromMarkRanges(all_mark_ranges);
        current_ranges.intersect(HalfIntervals::initializeFromMarkRanges(mark_ranges_for_task).negate());
        all_mark_ranges = current_ranges.convertToMarkRangesFinal();

        // std::sort(mark_ranges_for_task.begin(), mark_ranges_for_task.end());
        // std::sort(all_mark_ranges.begin(), all_mark_ranges.end());

        // while (!all_mark_ranges.empty())
        // {
        //     auto current_range = all_mark_ranges.front();
        //     if (current_range.begin >= mark_ranges_for_task.back().end)
        //     {
        //         all_mark_ranges.pop_front();
        //         continue;
        //     }
        //     break;
        // }
    }

    auto size_predictor = (preferred_block_size_bytes == 0) ? nullptr
        : getSizePredictor(data_part, task_columns, sample_block);

    task = std::make_unique<MergeTreeReadTask>(
        data_part, mark_ranges_for_task, part_index_in_query, ordered_names, column_name_set, task_columns,
        prewhere_info && prewhere_info->remove_prewhere_column,
        std::move(size_predictor));

    return true;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part);
    throw;
}

}
