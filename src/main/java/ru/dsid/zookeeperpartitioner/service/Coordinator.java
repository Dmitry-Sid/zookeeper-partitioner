package ru.dsid.zookeeperpartitioner.service;

import java.util.Set;

public interface Coordinator {
    Set<Integer> getAssignedPartitions();

    boolean isRebalancing();
}
