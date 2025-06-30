package ru.dsid.zookeeperpartitioner.service;

import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;

public interface ZooKeeperConnector {

    ZooKeeper connect(String host) throws IOException, InterruptedException;
}
