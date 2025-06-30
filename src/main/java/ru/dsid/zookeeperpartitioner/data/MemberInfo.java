package ru.dsid.zookeeperpartitioner.data;

import lombok.*;

import java.util.Set;

@Setter
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class MemberInfo {
    private int workersCount;
    private boolean leader;
    private boolean rebalancing;
    private boolean acceptedNewPartitions;
    private int totalWorkers;
    private Set<Integer> assignedPartitions;
}
