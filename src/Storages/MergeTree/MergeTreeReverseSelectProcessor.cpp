#include <Storages/MergeTree/MergeTreeReverseSelectProcessor.h>
#include <Storages/MergeTree/IntersectionsIndexes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

bool MergeTreeReverseSelectProcessor::getNewTaskImpl()
try
{
    if (chunks.empty() && all_mark_ranges.empty())
        return false;

    /// We have some blocks to return in buffer.
    /// Return true to continue reading, but actually don't create a task.
    if (all_mark_ranges.empty())
        return true;

    if (!reader)
        initializeReaders();

    /// Read ranges from right to left.
    auto mark_ranges_for_task = MarkRanges{};
    mark_ranges_for_task.emplace_front(std::move(all_mark_ranges.back()));
    all_mark_ranges.pop_back();

    if (pool)
    {
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
    }

    auto size_predictor = (preferred_block_size_bytes == 0) ? nullptr
        : getSizePredictor(data_part, task_columns, sample_block);

    task = std::make_unique<MergeTreeReadTask>(
        data_part, mark_ranges_for_task, part_index_in_query, ordered_names, column_name_set,
        task_columns, prewhere_info && prewhere_info->remove_prewhere_column,
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

MergeTreeBaseSelectProcessor::BlockAndRowCount MergeTreeReverseSelectProcessor::readFromPart()
{
    BlockAndRowCount res;

    if (!chunks.empty())
    {
        res = std::move(chunks.back());
        chunks.pop_back();
        return res;
    }

    if (!task->range_reader.isInitialized())
        initializeRangeReaders(*task);

    while (!task->isFinished())
    {
        auto chunk = readFromPartImpl();
        chunks.push_back(std::move(chunk));
    }

    if (chunks.empty())
        return {};

    res = std::move(chunks.back());
    chunks.pop_back();

    return res;
}

}
