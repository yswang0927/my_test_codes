package loadbalancer;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.stream.Collectors;

/**
 * OK: 这个行为和nginx保持一致了.
 */
public class WeightedRoundRobbinServerGroup2 {
    private final List<ServerNode> mainServers = new CopyOnWriteArrayList<>();
    private final List<ServerNode> backupServers = new CopyOnWriteArrayList<>();

    // 主锁保护服务器列表结构和 usingBackup 标志的读取
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public WeightedRoundRobbinServerGroup2() {}

    public WeightedRoundRobbinServerGroup2(Collection<ServerNode> serverList) {
        if (serverList != null && serverList.size() > 0) {
            this.mainServers.addAll(serverList.stream().filter(node -> node != null && !node.isBackup).collect(Collectors.toList()));
            this.backupServers.addAll(serverList.stream().filter(node -> node != null && node.isBackup).collect(Collectors.toList()));
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

    public void removeServer(ServerNode server) {
        if (server == null) {
            return;
        }
        if (server.isBackup) {
            this.backupServers.remove(server);
        } else {
            this.mainServers.remove(server);
        }
    }

    public ServerNode getNextServer() {
        this.lock.readLock().lock();
        try {
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
                if (server.isDown) {
                    continue;
                }

                // 定义: 当一个节点在 fail_timeout 时间内失败次数达到 max_fails 时, 它会被判断为不可用;
                // 并在 fail_timeout 时间后再次尝试
                if (server.maxFails > 0
                        && server.fails >= server.maxFails
                        && (now - server.checkedTimestamp) <= server.failTimeoutMillis) {
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
                        System.out.println("服务节点进入暂时不可用状态: " + server.endpoint);
                    }
                }

                if (server.effectiveWeight < 0) {
                    server.effectiveWeight = 0;
                }

            } else {
                if (server.fails > 0) {
                    server.fails = 0;
                }
            }

        } finally {
            server.lock.unlock();
        }

    }

}
