package loadbalancer;

import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WeightedRoundRobbinServerGroup extends ServerGroup {
    // 读写锁保护共享资源
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    @Override
    public void addServer(ServerNode server) {
        lock.writeLock().lock();
        try {
            super.addServer(server);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public ServerNode getNextServer() {
        lock.writeLock().lock();
        try {
            ServerNode best = null;
            int total = 0;
            long now = System.currentTimeMillis();
            List<ServerNode> serversToUse;

            // 优先使用主服务器，除非它们都不可用
            if (!usingBackup) {
                serversToUse = mainServers;
            } else {
                serversToUse = backupServers;
            }

            // 单服务器优化：如果只有一个服务器且可用，直接返回
            /*if (serversToUse.size() == 1) {
                ServerNode singleServer = serversToUse.get(0);
                if (!singleServer.isDownState &&
                        !(singleServer.maxFails > 0 && singleServer.fails >= singleServer.maxFails &&
                                (now - singleServer.checkedTimestamp) <= singleServer.failTimeoutMillis)
                        && !(singleServer.maxConns > 0 && singleServer.conns.get() >= singleServer.maxConns)) {
                    singleServer.conns.incrementAndGet();
                    return singleServer;
                }
            }*/

            // 如果当前使用的服务器组为空，则尝试使用另一个
            if (serversToUse.isEmpty()) {
                if (!usingBackup && !backupServers.isEmpty()) {
                    serversToUse = backupServers;
                    usingBackup = true;
                    System.out.println("Switching to backup servers");
                } else {
                    return null; // 没有可用的服务器
                }
            }

            for (ServerNode server : serversToUse) {
                if (server.isDownState) {
                    continue;
                }

                // 如果失败次数已经超过设定的阈值，且仍在失败超时周期内，则认为此节点处于失败状态
                if (server.maxFails > 0 && server.fails >= server.maxFails
                        && (now - server.checkedTimestamp) <= server.failTimeoutMillis) {
                    server.isDownState = true;
                    continue;
                }

                // 如果限制了最大连接数，且超过阈值了，跳过此节点
                if (server.maxConns > 0 && server.conns >= server.maxConns) {
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
            }

            // 如果没有找到可用的主服务器，尝试使用备份服务器
            if (best == null && !usingBackup && !backupServers.isEmpty()) {
                usingBackup = true;
                System.out.println("Switching to backup servers");
                return getNextServer();
            }

            if (best != null) {
                best.currentWeight -= total;
                best.conns++;
                if ((now - best.checkedTimestamp) > best.failTimeoutMillis) {
                    best.checkedTimestamp = now;
                }
            }

            return best;
        } finally {
            lock.writeLock().unlock();
        }
    }

    // 释放服务器连接
    public void freeServer(ServerNode server, boolean failed) {
        lock.writeLock().lock();
        try {
            server.conns--;

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
                    server.effectiveWeight = server.weight; // 重置有效权重
                    System.out.println("Server marked as up: " + server.endpoint);
                }
            }

            // 检查是否应该从备份服务器切换回主服务器
            if (usingBackup) {
                boolean allMainDown = true;
                long now = System.currentTimeMillis();
                for (ServerNode mainServer : mainServers) {
                    if (!mainServer.isDownState
                            && !(mainServer.maxFails > 0 && mainServer.fails >= mainServer.maxFails
                            && (now - mainServer.checkedTimestamp) <= mainServer.failTimeoutMillis)) {
                        allMainDown = false;
                        break;
                    }
                }

                if (!allMainDown) {
                    usingBackup = false;
                    System.out.println("Switching back to main servers");
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

}
