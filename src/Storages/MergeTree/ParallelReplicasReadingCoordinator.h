#pragma once

#include <memory>
#include <Storages/MergeTree/RequestResponse.h>

namespace DB
{

class ParallelReplicasReadingCoordinator
{
public:
    class ImplInterface;

    explicit ParallelReplicasReadingCoordinator(size_t replicas_count_);
    ~ParallelReplicasReadingCoordinator();

    void handleInitialAllRangesAnnouncement(InitialAllRangesAnnouncement);
    ParallelReadResponse handleRequest(ParallelReadRequest request);

private:
    std::unique_ptr<ImplInterface> pimpl;
};

using ParallelReplicasReadingCoordinatorPtr = std::shared_ptr<ParallelReplicasReadingCoordinator>;

}
