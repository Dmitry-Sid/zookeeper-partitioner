package ru.dsid.zookeeperpartitioner.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.AllArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.dsid.zookeeperpartitioner.data.MemberInfo;
import ru.dsid.zookeeperpartitioner.service.Coordinator;
import ru.dsid.zookeeperpartitioner.service.ZooKeeperConnector;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Service
public class CoordinatorImpl implements Coordinator {
    private final ObjectMapper objectMapper;
    private final ZooKeeperConnector zooKeeperConnector;
    private final String zooKeeperHost;
    private final int workersCount;
    private final long checkStatusRate;

    private volatile ZooKeeper zooKeeper;
    private volatile String currentNode;
    private volatile List<String> members;
    private volatile MemberInfo memberInfo;

    public CoordinatorImpl(ObjectMapper objectMapper,
                           ZooKeeperConnector zooKeeperConnector,
                           @Value("${zoo-keeper.host}") String zooKeeperHost,
                           @Value("${app.workers-count}") int workersCount,
                           @Value("${app.check-status-rate}") long checkStatusRate) {
        this.objectMapper = objectMapper;
        this.zooKeeperConnector = zooKeeperConnector;
        this.zooKeeperHost = zooKeeperHost;
        this.workersCount = workersCount;
        this.checkStatusRate = checkStatusRate;
    }

    @PostConstruct
    private void init() throws Exception {
        zooKeeper = zooKeeperConnector.connect(zooKeeperHost);
        connectToGroup();
        checkStatus();
        Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
            try {
                checkStatus();
            } catch (Throwable e) {
                log.error("error while checking status", e);
                initMemberInfo();
            }
        }, checkStatusRate, checkStatusRate, TimeUnit.MILLISECONDS);
    }

    private void connectToGroup() throws InterruptedException, KeeperException, JsonProcessingException {
        log.debug("connecting to group...");

        initMemberInfo();
        currentNode = zooKeeper.create("/partitioner/node", objectMapper.writeValueAsBytes(memberInfo),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        log.debug("connected to group successfully");
    }

    private void initMemberInfo() {
        memberInfo = MemberInfo.builder()
                .rebalancing(true)
                .workersCount(workersCount)
                .build();

        log.debug("member info has been initialized");
    }

    @SneakyThrows
    private void checkStatus() {
        log.trace("checking status...");

        checkNodeExist();
        checkNewMembers();

        if (!memberInfo.isLeader() && checkLeaderRebalancing()) {
            receiveAssignedPartitionsAsFollower();
        }

        if (memberInfo.isRebalancing()) {
            if (!memberInfo.isLeader()) {
                memberInfo.setRebalancing(!checkLeaderRebalancing());
            } else if (checkAllMembersAcceptedNewPartitions()) {
                stopLeaderRebalancing();
            }
        }
    }

    private void checkNodeExist() throws InterruptedException, KeeperException, JsonProcessingException {
        log.trace("checking node exist...");
        if (zooKeeper.exists(currentNode, null) == null) {
            connectToGroup();
        }
    }

    private void checkNewMembers() throws InterruptedException, KeeperException, IOException {
        final List<String> nodes = zooKeeper.getChildren("/partitioner/nodes", false);
        Collections.sort(nodes);

        if (!nodes.equals(members)) {
            log.debug("nodes doesn't equal members");
            if (nodes.get(0).equals(currentNode)) {
                distributePartitionsAsLeader();
            } else {
                receiveAssignedPartitionsAsFollower();
            }
            members = nodes;
        }
    }

    private void distributePartitionsAsLeader() throws InterruptedException, KeeperException, IOException {
        log.debug("current node is a leader");
        log.debug("distribution of the partitions started...");

        memberInfo.setLeader(true);
        memberInfo.setRebalancing(true);
        final boolean rebalancing = members.size() > 1;

        int totalWorkers = 0;
        final List<MemberMetaData> memberMetaDataList = new ArrayList<>();
        for (String node : members) {
            final MemberInfo memberInfo;
            if (currentNode.equals(node)) {
                memberInfo = this.memberInfo;
                memberInfo.setLeader(true);
                memberInfo.setAcceptedNewPartitions(true);
            } else {
                memberInfo = objectMapper.readValue(zooKeeper.getData(node, null, null), MemberInfo.class);
                memberInfo.setLeader(false);
                memberInfo.setAcceptedNewPartitions(false);
            }
            memberInfo.setRebalancing(rebalancing);
            final int version = zooKeeper.exists(node, null).getVersion();

            memberInfo.setAssignedPartitions(IntStream.range(totalWorkers, totalWorkers + memberInfo.getWorkersCount())
                    .boxed().collect(Collectors.toSet()));

            memberMetaDataList.add(new MemberMetaData(node, version, memberInfo));
            totalWorkers += memberInfo.getWorkersCount();
        }

        for (MemberMetaData memberMetaData : memberMetaDataList) {
            memberMetaData.memberInfo.setTotalWorkers(totalWorkers);
            zooKeeper.setData(memberMetaData.node, objectMapper.writeValueAsBytes(memberInfo), memberMetaData.version);
        }

        log.debug("distribution of the partitions successfully finished");
    }

    private void receiveAssignedPartitionsAsFollower() throws InterruptedException, KeeperException, IOException {
        log.debug("receiving assigned partitions as follower...");

        final int version = zooKeeper.exists(currentNode, null).getVersion();
        memberInfo = objectMapper.readValue(zooKeeper.getData(currentNode, null, null), MemberInfo.class);
        memberInfo.setAcceptedNewPartitions(true);
        zooKeeper.setData(currentNode, objectMapper.writeValueAsBytes(memberInfo), version);

        log.debug("successfully received assigned partitions as follower");
    }

    private boolean checkAllMembersAcceptedNewPartitions() {
        return members.stream().filter(n -> !n.equals(currentNode)).allMatch(m -> {
            try {
                return objectMapper.readValue(zooKeeper.getData(m, null, null), MemberInfo.class).isAcceptedNewPartitions();
            } catch (IOException | KeeperException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void stopLeaderRebalancing() throws InterruptedException, KeeperException, IOException {
        log.debug("stopping leader rebalancing...");
        zooKeeper.setData(currentNode, objectMapper.writeValueAsBytes(memberInfo), zooKeeper.exists(currentNode, null).getVersion());
        memberInfo.setRebalancing(false);
        log.debug("leader successfully rebalanced");
    }

    private boolean checkLeaderRebalancing() throws InterruptedException, KeeperException, IOException {
        return objectMapper.readValue(zooKeeper.getData(members.get(0), null, null), MemberInfo.class).isRebalancing();
    }

    @Override
    public Set<Integer> getAssignedPartitions() {
        if (memberInfo.isRebalancing()) {
            return Collections.emptySet();
        }
        return memberInfo.getAssignedPartitions();
    }

    @Override
    public boolean isRebalancing() {
        return memberInfo.isRebalancing();
    }

    @PreDestroy
    public void destroy() throws InterruptedException {
        zooKeeper.close();
    }

    @AllArgsConstructor
    private static class MemberMetaData {
        private final String node;
        private final int version;
        private final MemberInfo memberInfo;
    }
}
