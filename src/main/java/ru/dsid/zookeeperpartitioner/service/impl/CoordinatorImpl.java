package ru.dsid.zookeeperpartitioner.service.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import ru.dsid.zookeeperpartitioner.service.Coordinator;
import ru.dsid.zookeeperpartitioner.service.ZooKeeperConnector;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@Service
public class CoordinatorImpl implements Coordinator {
    private static final String ROOT = "/partitioner";
    private final ObjectMapper objectMapper;
    private final ZooKeeperConnector zooKeeperConnector;
    private final String zooKeeperHost;
    private final int workersCount;
    private final long checkStatusRate;
    private final BlockingQueue<Integer> partitionsQueue = new LinkedBlockingQueue<>();
    private final ThreadLocal<Integer> partitionThreadLocal = new ThreadLocal<>();

    private volatile ZooKeeper zooKeeper;
    private volatile String currentNode;
    private volatile List<String> members = Collections.emptyList();
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
        try {
            zooKeeper.create(ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException.NodeExistsException e) {
            // ignore
        }
        currentNode = zooKeeper.create(ROOT + "/node", objectMapper.writeValueAsBytes(memberInfo),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        log.debug("connected to group successfully with nodePath {}", currentNode);
    }

    private void initMemberInfo() {
        memberInfo = MemberInfo.builder()
                .rebalancing(true)
                .workersCount(workersCount)
                .assignedPartitions(Collections.emptySet())
                .build();

        log.debug("member info has been initialized");
    }

    @SneakyThrows
    private void checkStatus() {
        log.trace("checking status...");

        checkNodeExist();
        checkNewMembers();

        if (memberInfo.isRebalancing() && getActiveTasks() == 0) {
            if (memberInfo.isLeader()) {
                if (!memberInfo.isAcceptedNewPartitions()) {
                    distributePartitionsAsLeader();
                }

                if (memberInfo.isRebalancing() && checkAllFollowersAcceptedNewPartitions()) {
                    stopLeaderRebalancing();
                }
            } else {
                if (!memberInfo.isAcceptedNewPartitions()) {
                    receiveAssignedPartitionsAsFollower();
                }
                if (memberInfo.isAcceptedNewPartitions() && !checkLeaderRebalancing()) {
                    memberInfo.setRebalancing(false);
                    log.debug("follower successfully rebalanced");
                }
            }
        }
    }

    @Override
    public int getActiveTasks() {
        return getAssignedPartitions().size() - partitionsQueue.size();
    }

    private void checkNodeExist() throws InterruptedException, KeeperException, JsonProcessingException {
        log.trace("checking node exist...");
        if (zooKeeper.exists(currentNode, null) == null) {
            connectToGroup();
        }
    }

    private void checkNewMembers() throws InterruptedException, KeeperException, IOException {
        final List<String> members = zooKeeper.getChildren(ROOT, false).stream().sorted().map(n -> ROOT + "/" + n)
                .collect(Collectors.toList());

        if (!members.equals(this.members)) {
            log.debug("members list changed");
            memberInfo.setRebalancing(true);
            memberInfo.setLeader(members.get(0).equals(currentNode));
            memberInfo.setAcceptedNewPartitions(false);

            if (memberInfo.isLeader()) {
                log.debug("current node is a leader");
            }
            this.members = members;
        }
    }

    private void distributePartitionsAsLeader() throws InterruptedException, KeeperException, IOException {
        checkNodeIsLeader();

        log.debug("distribution of the partitions started...");
        final boolean rebalancing = members.size() > 1;

        int totalWorkers = 0;
        final List<MemberMetaData> memberMetaDataList = new ArrayList<>();
        for (int i = 0; i < members.size(); i++) {
            final String node = members.get(i);
            final MemberInfo memberInfo;
            if (i == 0) {
                memberInfo = this.memberInfo;
                memberInfo.setAcceptedNewPartitions(true);
            } else {
                memberInfo = objectMapper.readValue(zooKeeper.getData(node, null, null), MemberInfo.class);
                memberInfo.setLeader(false);
                memberInfo.setAcceptedNewPartitions(false);
            }
            memberInfo.setRebalancing(rebalancing);

            memberInfo.setAssignedPartitions(IntStream.range(totalWorkers, totalWorkers + memberInfo.getWorkersCount())
                    .boxed().collect(Collectors.toSet()));

            memberMetaDataList.add(new MemberMetaData(node, -1, memberInfo));
            totalWorkers += memberInfo.getWorkersCount();

            if (log.isDebugEnabled()) {
                log.debug("the node {} is assigned partitions: {}", node, Arrays.toString(memberInfo.getAssignedPartitions().toArray()));
            }
        }

        for (final MemberMetaData memberMetaData : memberMetaDataList) {
            memberMetaData.memberInfo.setTotalWorkers(totalWorkers);
            zooKeeper.setData(memberMetaData.node, objectMapper.writeValueAsBytes(memberMetaData.memberInfo), memberMetaData.version);
        }

        fillPartitionsQueue();

        log.debug("distribution of the partitions successfully finished");
    }

    private void checkNodeIsLeader() {
        if (!memberInfo.isLeader()) {
            throw new IllegalStateException("node must be a leader!");
        }
    }

    private void checkNodeIsFollower() {
        if (memberInfo.isLeader()) {
            throw new IllegalStateException("node must be a follower!");
        }
    }

    private void receiveAssignedPartitionsAsFollower() throws InterruptedException, KeeperException, IOException {
        checkNodeIsFollower();

        if (memberInfo.isAcceptedNewPartitions()) {
            throw new IllegalStateException("node has already received assigned partitions");
        }

        log.debug("receiving assigned partitions as follower...");

        final int currentNodeVersion = zooKeeper.exists(currentNode, null).getVersion();
        final MemberInfo memberInfo = objectMapper.readValue(zooKeeper.getData(currentNode, null, null), MemberInfo.class);

        if (memberInfo.isAcceptedNewPartitions() || CollectionUtils.isEmpty(memberInfo.getAssignedPartitions())) {
            log.debug("there is no assigned partitions yet");
            return;
        }

        memberInfo.setAcceptedNewPartitions(true);
        zooKeeper.setData(currentNode, objectMapper.writeValueAsBytes(memberInfo), currentNodeVersion);
        this.memberInfo = memberInfo;
        fillPartitionsQueue();

        if (log.isDebugEnabled()) {
            log.debug("the node {} is assigned partitions: {}", currentNode, Arrays.toString(memberInfo.getAssignedPartitions().toArray()));
        }

        log.debug("successfully received assigned partitions as follower");
    }

    private boolean checkAllFollowersAcceptedNewPartitions() throws InterruptedException, KeeperException, IOException {
        for (int i = 1; i < members.size(); i++) {
            if (!objectMapper.readValue(zooKeeper.getData(members.get(i), null, null), MemberInfo.class).isAcceptedNewPartitions()) {
                return false;
            }
        }
        return true;
    }

    private boolean checkLeaderRebalancing() throws InterruptedException, KeeperException, IOException {
        return objectMapper.readValue(zooKeeper.getData(members.get(0), null, null), MemberInfo.class).isRebalancing();
    }


    private void stopLeaderRebalancing() throws InterruptedException, KeeperException, IOException {
        checkNodeIsLeader();

        log.debug("stopping leader rebalancing...");
        memberInfo.setRebalancing(false);
        for (final String node : members) {
            final MemberInfo memberInfo = objectMapper.readValue(zooKeeper.getData(node, null, null), MemberInfo.class);
            memberInfo.setRebalancing(false);
            zooKeeper.setData(node, objectMapper.writeValueAsBytes(memberInfo), -1);
        }

        log.debug("leader successfully rebalanced");
    }

    private synchronized void fillPartitionsQueue() {
        partitionsQueue.clear();
        partitionsQueue.addAll(memberInfo.getAssignedPartitions());
    }

    @Override
    public int startTask() throws InterruptedException {
        while (isRebalancing()) {
            Thread.sleep(100);
        }
        final Integer partition = partitionsQueue.take();
        partitionThreadLocal.set(partition);
        return partition;
    }

    @Override
    public void finishTask() {
        final Integer partition = partitionThreadLocal.get();
        if (partition != null) {
            partitionsQueue.add(partition);
            partitionThreadLocal.set(null);
        }
    }

    @Override
    public int getWorkersCount() {
        return memberInfo.getWorkersCount();
    }

    @Override
    public boolean isLeader() {
        return memberInfo.isLeader();
    }

    @Override
    public boolean isRebalancing() {
        return memberInfo.isRebalancing();
    }

    @Override
    public boolean isAcceptedNewPartitions() {
        return memberInfo.isAcceptedNewPartitions();
    }

    @Override
    public int getTotalWorkers() {
        return memberInfo.getTotalWorkers();
    }

    @Override
    public Set<Integer> getAssignedPartitions() {
        return memberInfo.getAssignedPartitions();
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

    @Setter
    @Getter
    @NoArgsConstructor
    @AllArgsConstructor
    @Builder
    private static class MemberInfo {
        private volatile int workersCount;
        private volatile boolean leader;
        private volatile boolean rebalancing;
        private volatile boolean acceptedNewPartitions;
        private volatile int totalWorkers;
        private volatile Set<Integer> assignedPartitions;
    }
}
