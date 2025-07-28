package loadbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WeightedRoundRobbinServerGroup2 {
    private final List<ServerNode> mainServers = new ArrayList<>();
    private final List<ServerNode> backupServers = new ArrayList<>();

    // 主锁保护服务器列表结构和 usingBackup 标志的读取
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public WeightedRoundRobbinServerGroup2() {}

    public WeightedRoundRobbinServerGroup2(Collection<ServerNode> serverList) {
        if (serverList != null) {
            for (ServerNode serverNode : serverList) {
                addServer(serverNode);
            }
        }
    }

    public void addServer(ServerNode server) {
        if (server == null || server.isDown) {
            return;
        }

        this.lock.writeLock().lock();
        try {
            if (server.isBackup) {
                this.backupServers.add(server);
            } else {
                this.mainServers.add(server);
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public ServerNode getNextServer() {
        this.lock.readLock().lock();
        try {
            if (mainServers.isEmpty()) {
                return findBestServerNode(backupServers);
            }

            ServerNode serverNode = findBestServerNode(mainServers);
            if (serverNode != null) {
                return serverNode;
            }

            return findBestServerNode(backupServers);
        } finally {
            this.lock.readLock().unlock();
        }
    }

    private ServerNode findBestServerNode(List<ServerNode> serverNodes) {
        if (serverNodes.isEmpty()) {
            return null;
        }

        ServerNode best = null;
        int total = 0;
        long now = System.currentTimeMillis();

        for (ServerNode server : serverNodes) {
            // --- 对单个 server 加锁进行状态检查和修改 ---
            server.lock.lock();
            try {
                if (server.isDownState) {
                    continue;
                }

                // 如果失败次数已经超过设定的阈值，且仍在失败超时周期内，则认为此节点处于失败状态
                if (server.maxFails > 0
                        && server.fails >= server.maxFails
                        && (now - server.checkedTimestamp) <= server.failTimeoutMillis) {
                    server.isDownState = true;
                    continue;
                }

                // 如果限制了最大连接数，且超过阈值了，跳过此节点
                if (server.maxConns > 0 && server.conns.get() >= server.maxConns) {
                    continue;
                }

                server.currentWeight += server.effectiveWeight;
                total += server.effectiveWeight;

                if (server.effectiveWeight < server.weight) {
                    server.effectiveWeight++;
                }

                if (best == null || server.currentWeight > best.currentWeight) {
                    best = server;
                }
            } finally {
                server.lock.unlock();
            }
        }

        if (best == null) {
            return null;
        }

        best.lock.lock();
        try {
            best.conns.incrementAndGet();
            best.currentWeight -= total;

            if ((now - best.checkedTimestamp) > best.failTimeoutMillis) {
                best.checkedTimestamp = now;
            }
        } finally {
            best.lock.unlock();
        }

        return best;
    }

    // 释放服务器连接
    public void freeServer(ServerNode server, boolean failed) {
        if (server == null) {
            return;
        }

        server.lock.lock();
        try {
            server.conns.decrementAndGet();

            if (failed) {
                server.fails++;
                server.checkedTimestamp = System.currentTimeMillis();

                if (server.maxFails > 0) {
                    server.effectiveWeight -= server.weight / server.maxFails;
                    if (server.fails >= server.maxFails) {
                        server.isDownState = true;
                        System.out.println("Upstream server temporarily disabled: " + server.endpoint);
                    }
                }
                if (server.effectiveWeight < 0) {
                    server.effectiveWeight = 0;
                }

            } else {
                if (server.fails > 0) {
                    server.fails = 0;
                }

                // 如果服务器之前被标记为down，现在请求成功，将其恢复
                if (server.isDownState) {
                    server.isDownState = false;
                    // 不立即恢复到满权重，让它逐步恢复
                    //server.effectiveWeight = server.weight; // 重置有效权重
                    if (server.effectiveWeight <= 0) {
                        server.effectiveWeight = 1; // 给一个最小值开始恢复
                    }
                    System.out.println("Server marked as up: " + server.endpoint);
                }
            }

        } finally {
            server.lock.unlock();
        }

    }

}
