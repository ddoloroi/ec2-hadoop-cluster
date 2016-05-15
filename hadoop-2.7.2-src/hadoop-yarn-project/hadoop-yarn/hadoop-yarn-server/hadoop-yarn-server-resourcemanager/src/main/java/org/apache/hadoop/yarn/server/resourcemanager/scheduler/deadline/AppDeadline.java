package org.apache.hadoop.yarn.server.resourcemanager.scheduler.deadline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

import java.util.Date;

public class AppDeadline {
    private static final Log LOG = LogFactory.getLog(AppDeadline.class);
    private static Resource minAllocation;

    private long deadline;
    private long estimatedFinishTime;
    private long slack;
    private long avgTimePerMinAllocation;
    private int completedContainers;

    private ApplicationId applicationId;

    public AppDeadline(ApplicationId applicationId, int deadline) {
        this.applicationId = applicationId;
        this.deadline = System.currentTimeMillis() / 1000 + deadline;
    }

    public ApplicationId getApplicationId() {
        return applicationId;
    }

    public long getDeadline() {
        return deadline;
    }

    public void estimateFinishTime(ResourceRequest resourceRequest) {
        int minAllocations = resourceRequest.getCapability().getMemory() * resourceRequest.getCapability().getVirtualCores() *
                resourceRequest.getNumContainers();
        estimatedFinishTime = System.currentTimeMillis() / 1000 + avgTimePerMinAllocation * minAllocations;
        slack = deadline - estimatedFinishTime;
        LOG.info("Estimated finish time = " + new Date(estimatedFinishTime * 1000) +
            " deadline = " + new Date(deadline * 1000));
    }

    public void addCompletedContainer(RMContainer rmContainer) {
        long runTime = (rmContainer.getFinishTime() - rmContainer.getCreationTime()) / 1000;
        Resource usedResource = rmContainer.getAllocatedResource();
        int factor = (usedResource.getMemory() / minAllocation.getMemory()) *
                (usedResource.getVirtualCores() / minAllocation.getVirtualCores());
        runTime *= factor;
        avgTimePerMinAllocation = (avgTimePerMinAllocation * completedContainers + runTime) / (completedContainers + 1);
        completedContainers += 1;

        LOG.info("addCompletedContainer: runTime=" + runTime / factor +
            " factor=" + factor +
            " adjustedRunTime=" + runTime +
            " avgTimePerMinAllocation=" + avgTimePerMinAllocation);
    }

    public static void setMinAllocation(Resource resource) {
       minAllocation = resource;
    }
}
