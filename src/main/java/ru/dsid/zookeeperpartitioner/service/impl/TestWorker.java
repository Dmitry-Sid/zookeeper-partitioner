package ru.dsid.zookeeperpartitioner.service.impl;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.dsid.zookeeperpartitioner.service.Coordinator;
import ru.dsid.zookeeperpartitioner.service.Worker;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Service
public class TestWorker implements Worker {
    private final Coordinator coordinator;
    private final int workersCount;
    private final ExecutorService executorService;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();

    private volatile boolean running;

    public TestWorker(Coordinator coordinator, @Value("${app.workers-count}") int workersCount) {
        this.coordinator = coordinator;
        this.workersCount = workersCount;
        this.executorService = Executors.newFixedThreadPool(workersCount);
    }

    @PostConstruct
    public void init() {
        start();
    }

    @Override
    public void start() {
        running = true;
        for (int i = 0; i < workersCount; i++) {
            executorService.submit(() -> {
                while (running) {
                    try {
                        try {
                            final int partition = coordinator.startTask();
                            log.info("making some stub work for partition: {}, totalWorkers: {}", partition, coordinator.getTotalWorkers());
                            Thread.sleep(1000 + random.nextLong(10_000));
                        } finally {
                            coordinator.finishTask();
                        }
                    } catch (InterruptedException e) {
                        log.error("interrupted", e);
                    }
                }
            });
        }
    }

    @Override
    public void stop() {
        running = false;
    }

    @PreDestroy
    public void destroy() {
        stop();
    }
}
