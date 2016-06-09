package org.apache.hadoop.yarn.server.resourcemanager.scheduler.deadline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

import java.util.Date;
import java.util.PriorityQueue;
import java.util.Queue;

public class AppDeadline implements Comparable<AppDeadline>{
    private static final Log LOG = LogFactory.getLog(AppDeadline.class);
    private static Resource minAllocation;

    private long deadline;
    private long estimatedFinishTime;
    private long slack;
    private long avgTimePerMinAllocation;
    private int maxQueueLength = 3;
    private long queueSum = 0;
    private Queue<Long> lastRunTimes = new PriorityQueue<>();
    private ResourceRequest resourceRequest;
    private int containersCreated = 0;

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
        this.resourceRequest = resourceRequest;
        int minAllocations = (resourceRequest.getCapability().getMemory() / minAllocation.getMemory()) *
                (resourceRequest.getCapability().getVirtualCores() / minAllocation.getVirtualCores()) *
                resourceRequest.getNumContainers();
        LOG.info("minAllocations memory=" + resourceRequest.getCapability().getMemory() +
            " vCores=" + resourceRequest.getCapability().getVirtualCores() +
            " containers=" + resourceRequest.getNumContainers());
        estimatedFinishTime = System.currentTimeMillis() / 1000 + avgTimePerMinAllocation * minAllocations;
        slack = deadline - estimatedFinishTime;
        LOG.info("Estimated finish time = " + new Date(estimatedFinishTime * 1000) +
            " deadline = " + new Date(deadline * 1000));
    }

    public void estimateFinishTime() {
        estimateFinishTime(this.resourceRequest);
    }

    public void addCompletedContainer(RMContainer rmContainer) {
        long runTime = (rmContainer.getFinishTime() - rmContainer.getCreationTime()) / 1000;
        Resource usedResource = rmContainer.getAllocatedResource();
        int factor = (usedResource.getMemory() / minAllocation.getMemory()) *
                (usedResource.getVirtualCores() / minAllocation.getVirtualCores());
        runTime *= factor;
        queueSum += runTime;
        lastRunTimes.add(runTime);
        if (lastRunTimes.size() > maxQueueLength) {
            queueSum -= lastRunTimes.remove();
        }
        avgTimePerMinAllocation = queueSum / lastRunTimes.size();

        LOG.info("addCompletedContainer: runTime=" + runTime / factor +
            " factor=" + factor +
            " adjustedRunTime=" + runTime +
            " avgTimePerMinAllocation=" + avgTimePerMinAllocation);
    }

    public void increaseCreatedContainers() {
        containersCreated += 1;
        resourceRequest.setNumContainers(Math.max(0, resourceRequest.getNumContainers() - 1));
    }

    public int compareTo(AppDeadline appDeadline) {
        if (containersCreated < 2)
            return -1;

        if (slack < appDeadline.slack)
            return -1;
        else if (slack > appDeadline.slack)
            return 1;
        else
            return 0;
    }

    public static void setMinAllocation(Resource resource) {
       minAllocation = resource;
    }
}
