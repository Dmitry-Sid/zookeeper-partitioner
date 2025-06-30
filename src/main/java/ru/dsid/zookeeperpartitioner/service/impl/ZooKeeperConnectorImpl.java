package ru.dsid.zookeeperpartitioner.service.impl;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.stereotype.Service;
import ru.dsid.zookeeperpartitioner.service.ZooKeeperConnector;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Service
public class ZooKeeperConnectorImpl implements ZooKeeperConnector {

    @Override
    public ZooKeeper connect(String host) throws IOException, InterruptedException {
        final CountDownLatch connectionLatch = new CountDownLatch(1);
        final ZooKeeper zooKeeper = new ZooKeeper(host, 2000, we -> {
            if (we.getState() == Watcher.Event.KeeperState.SyncConnected) {
                connectionLatch.countDown();
            }
        });

        connectionLatch.await();
        return zooKeeper;
    }
}
