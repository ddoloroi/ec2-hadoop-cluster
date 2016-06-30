package org.apache.hadoop.yarn.server.resourcemanager.scheduler.deadline;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.math3.linear.MatrixUtils;
import org.apache.commons.math3.linear.RealMatrix;
import org.apache.commons.math3.stat.correlation.Covariance;
import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;

import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
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
    private Queue<Long> lastRunTimes = new LinkedList<>();
    private ArrayList<double []> covMatrixData = new ArrayList<>();
    private ResourceRequest resourceRequest;
    private int containersCreated = 0;
    private double memoryFactor = 1;
    private double vCoreFactor = 1;

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
        double minAllocations = (resourceRequest.getCapability().getMemory() / minAllocation.getMemory() * memoryFactor) *
                (resourceRequest.getCapability().getVirtualCores() / minAllocation.getVirtualCores() * vCoreFactor) *
                resourceRequest.getNumContainers();
        LOG.info("minAllocations memory=" + resourceRequest.getCapability().getMemory() +
            " vCores=" + resourceRequest.getCapability().getVirtualCores() +
            " containers=" + resourceRequest.getNumContainers());
        estimatedFinishTime = System.currentTimeMillis() / 1000 + (int)(avgTimePerMinAllocation * minAllocations);
        slack = deadline - estimatedFinishTime;
        LOG.info("Estimated finish time = " + new Date(estimatedFinishTime * 1000) +
            " deadline = " + new Date(deadline * 1000));
    }

    public void estimateFinishTime() {
        estimateFinishTime(this.resourceRequest);
    }

    synchronized public void addCompletedContainer(RMContainer rmContainer) {
        long runTime = (rmContainer.getFinishTime() - rmContainer.getCreationTime()) / 1000;
        Resource usedResource = rmContainer.getAllocatedResource();
        int vCores = usedResource.getVirtualCores() / minAllocation.getVirtualCores();
        int memory = usedResource.getMemory() / minAllocation.getMemory();
        covMatrixData.add(new double[] {vCores, memory, runTime});
        if (covMatrixData.size() > maxQueueLength) {
            covMatrixData.remove(0);
        }

        if (covMatrixData.size() > 1) {
            RealMatrix cor = new PearsonsCorrelation(covMatrixData.toArray(new double[0][0])).getCorrelationMatrix();
            vCoreFactor = Math.abs(Double.isNaN(cor.getEntry(0, 2)) ? 1.0 : cor.getEntry(0, 2));
            memoryFactor = Math.abs(Double.isNaN(cor.getEntry(1, 2)) ? 1.0 : cor.getEntry(1, 2));
        }

        double factor = (vCores * vCores) * (memory * memoryFactor);
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
