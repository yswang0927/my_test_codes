package loadbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WeightedRoundRobbinServerGroup {
    private final List<ServerNode> mainServers = new ArrayList<>();
    private final List<ServerNode> backupServers = new ArrayList<>();
    // 主服务器总权重
    private int mainTotalWeight = 0;
    // 备份服务器总权重
    private int backupTotalWeight = 0;
    // 是否已经切换到备份服务器
    private final AtomicBoolean usingBackup = new AtomicBoolean(false);

    // 主锁保护服务器列表结构和 usingBackup 标志的读取
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public WeightedRoundRobbinServerGroup() {}

    public WeightedRoundRobbinServerGroup(Collection<ServerNode> serverList) {
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
                this.backupTotalWeight += server.weight;
            } else {
                this.mainServers.add(server);
                this.mainTotalWeight += server.weight;
            }
        } finally {
            this.lock.writeLock().unlock();
        }
    }

    public ServerNode getNextServer() {
        this.lock.readLock().lock();

        // 优先使用主服务器，除非它们都不可用
        List<ServerNode> serversToUse = this.usingBackup.get() ? this.backupServers : this.mainServers;

        // 如果当前使用的服务器组为空，则尝试使用另一个
        if (serversToUse.size() == 0) {
            this.lock.readLock().unlock();
            this.lock.writeLock().lock();
            try {
                // 启用备用节点
                if (!this.usingBackup.get() && this.backupServers.size() > 0) {
                    this.usingBackup.set(true);
                    return getNextServer(); // 递归调用下
                } else {
                    return null; // 没有可用的服务器
                }
            } finally {
                this.lock.writeLock().unlock();
            }
        }

        long now = System.nanoTime() / 1_000_000;

        // 单服务器优化：如果只有一个服务器且可用，直接返回
        if (serversToUse.size() == 1) {
            ServerNode singleServer = serversToUse.get(0);
            singleServer.lock.lock();
            try {
                if (!singleServer.isDownState &&
                        !(singleServer.maxFails > 0 && singleServer.fails >= singleServer.maxFails &&
                                (now - singleServer.checkedTimestamp) <= singleServer.failTimeoutMillis)
                        && !(singleServer.maxConns > 0 && singleServer.conns.get() >= singleServer.maxConns)) {

                    singleServer.conns.incrementAndGet();
                    // 更新 checkedTimestamp, 如果超时了
                    if ((now - singleServer.checkedTimestamp) > singleServer.failTimeoutMillis) {
                        singleServer.checkedTimestamp = now;
                    }

                    this.lock.readLock().unlock();
                    return singleServer;
                }
            } finally {
                singleServer.lock.unlock();
            }
        }

        ServerNode best = null;
        int total = 0;

        for (ServerNode server : serversToUse) {
            if (server.isDownState) {
                continue;
            }

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

        lock.readLock().unlock();

        if (best != null) {
            best.lock.lock();
            try {
                best.currentWeight -= total;
                best.conns.incrementAndGet();
                if ((now - best.checkedTimestamp) > best.failTimeoutMillis) {
                    best.checkedTimestamp = now;
                }
            } finally {
                best.lock.unlock();
            }
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
                server.checkedTimestamp = System.nanoTime() / 1_000_000;

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
                    server.effectiveWeight = server.weight; // 重置有效权重
                    System.out.println("Server marked as up: " + server.endpoint);
                }
            }
        } finally {
            server.lock.unlock();
        }

        // 检查是否应该从备份服务器切换回主服务器
        if (usingBackup.get()) {
            lock.writeLock().lock();
            try {
                boolean allMainDown = true;
                long now = System.nanoTime() / 1_000_000;
                for (ServerNode mainServer : mainServers) {
                    mainServer.lock.lock();
                    try {
                        if (!mainServer.isDownState
                                && !(mainServer.maxFails > 0 && mainServer.fails >= mainServer.maxFails
                                && (now - mainServer.checkedTimestamp) <= mainServer.failTimeoutMillis)) {
                            allMainDown = false;
                            break;
                        }
                    } finally {
                        mainServer.lock.unlock();
                    }
                }

                if (!allMainDown) {
                    usingBackup.set(false);
                    System.out.println("Switching back to main servers");
                }
            } finally {
                lock.writeLock().unlock();
            }
        }

    }

}
