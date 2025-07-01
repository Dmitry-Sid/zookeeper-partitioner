package ru.dsid.zookeeperpartitioner.service;

import java.util.Set;

public interface Coordinator {
    void startTask();

    void endTask();

    int getWorkersCount();

    boolean isLeader();

    boolean isRebalancing();

    boolean isAcceptedNewPartitions();

    int getTotalWorkers();

    Set<Integer> getAssignedPartitions();
}
