#include <Storages/MergeTree/MergeTreeReadPoolParallelReplicas.h>
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


MergeTreeReadTaskPtr MergeTreeReadPoolParallelReplicas::getTask(size_t thread)
{
    (void)thread;
    return nullptr;
}

}
