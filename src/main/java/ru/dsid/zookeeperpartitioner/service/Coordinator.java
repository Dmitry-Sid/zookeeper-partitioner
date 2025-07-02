package ru.dsid.zookeeperpartitioner.service;

import java.util.Set;

public interface Coordinator {
    int getActiveTasks();

    /**
     * @return partition
     * @throws InterruptedException
     */
    int startTask() throws InterruptedException;

    void finishTask();

    int getWorkersCount();

    boolean isLeader();

    boolean isBalancing();

    boolean isAcceptedNewPartitions();

    int getTotalWorkers();

    Set<Integer> getAssignedPartitions();
}
