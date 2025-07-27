package loadbalancer;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Nginx's Smooth Weighted Round-Robin 算法实现。
 */
public class WeightedRoundRobinLoadBalancer {
    private List<ServerNode> servers;
    private final List<ServerNode> primaryServers;
    private final List<ServerNode> backupServers;

    private final ReentrantLock lock = new ReentrantLock();

    public WeightedRoundRobinLoadBalancer(List<ServerNode> servers) {
        this.primaryServers = new ArrayList<>();
        this.backupServers = new ArrayList<>();

        for (ServerNode server : servers) {
            if (server.isBackup) {
                this.backupServers.add(server);
            } else {
                this.primaryServers.add(server);
            }
        }
    }

    /**
     * Selects a server, respecting primary/backup server roles.
     * @return The best server to handle the request, or null if all are down.
     */
    public ServerNode selectServer() {
        lock.lock();
        try {
            // Pass 1: Attempt to select a primary server
            ServerNode bestServer = findBestPeer(false);
            // Pass 2: If no primary servers are available, attempt to select a backup server
            if (bestServer == null) {
                bestServer = findBestPeer(true);
            }

            return bestServer;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Selects the next server using Nginx's SWRR logic.
     * @return The best server to handle the request, or null if all are down.
     * @deprecated
     */
    @Deprecated
    public ServerNode selectServer2() {
        lock.lock();
        try {
            ServerNode bestServer = null;
            int totalEffectiveWeight = 0;

            // This loop mirrors the logic in ngx_http_upstream_get_round_robin_peer()
            for (ServerNode currentServer : servers) {
                // First, check server health
                if (currentServer.isDownState) {
                    long now = Instant.now().getEpochSecond();
                    if ((now - currentServer.checkedTimestamp) > currentServer.failTimeoutSeconds) {
                        currentServer.isDownState = false;
                        currentServer.fails = 0;
                        //System.out.printf("✅ Server %s is back up.%n", currentServer.address);
                    } else {
                        continue; // Server is still in its timeout period
                    }
                }

                // For all currently available servers:
                // 1. Their dynamic weight is increased by their static weight.
                currentServer.effectiveWeight += currentServer.weight;

                // 2. The total weight is summed up *dynamically* for this selection cycle.
                totalEffectiveWeight += currentServer.weight;

                // 3. The server with the highest dynamic weight is chosen as the best.
                // Nginx uses a simple > comparison. In case of a tie, the first one checked wins.
                if (bestServer == null || currentServer.effectiveWeight > bestServer.effectiveWeight) {
                    bestServer = currentServer;
                }
            }

            if (bestServer == null) {
                return null;
            }

            // 4. The chosen server is penalized by subtracting the dynamic total weight.
            bestServer.effectiveWeight -= totalEffectiveWeight;

            return bestServer;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Finds the best peer from either the primary or backup group.
     * @param isBackupGroup true to search backups, false to search primaries.
     * @return The selected server, or null if none are available in the group.
     */
    private ServerNode findBestPeer(boolean isBackupGroup) {
        ServerNode bestServer = null;
        int totalEffectiveWeight = 0;

        for (ServerNode currentServer : servers) {
            // Skip servers not in the target group (primary/backup) or marked 'down' in config
            if (currentServer.isBackup != isBackupGroup || currentServer.isDown) {
                continue;
            }

            // Health check for dynamic 'down' state (due to failures)
            if (currentServer.isDownState) {
                long now = Instant.now().getEpochSecond();
                if ((now - currentServer.checkedTimestamp) > currentServer.failTimeoutSeconds) {
                    currentServer.isDownState = false;
                    currentServer.fails = 0;
                    System.out.printf("✅ Server %s is back up.%n", currentServer.toString());
                } else {
                    continue; // Server is still in its timeout period
                }
            }

            currentServer.effectiveWeight += currentServer.weight;
            totalEffectiveWeight += currentServer.weight;

            if (bestServer == null || currentServer.effectiveWeight > bestServer.effectiveWeight) {
                bestServer = currentServer;
            }
        }

        if (bestServer == null) {
            return null;
        }

        bestServer.effectiveWeight -= totalEffectiveWeight;
        return bestServer;
    }

    public void reportConnectionOpened(ServerNode server) {
        if (server != null) {
            server.conns++;
        }
    }

    public void reportConnectionClosed(ServerNode server) {
        if (server != null) {
            server.conns--;
        }
    }

    /**
     * Reports a failure for a given server.
     * @param server The server that failed.
     */
    public void reportFailure(ServerNode server) {
        if (server == null || server.isDown) {
            return;
        }

        lock.lock();
        try {
            server.fails++;
            // The server is not marked as down until its fails exceed max_fails
            if (!server.isDownState && server.fails >= server.maxFails) {
                server.isDownState = true;
                server.checkedTimestamp = Instant.now().getEpochSecond(); // Mark the time it went down
                System.out.printf("🛑 Server %s is down (fails=%d). Will check again in %d seconds.%n",
                        server.endpoint, server.fails, server.failTimeoutSeconds);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * A successful connection on a healthy server resets its failure count.
     * @param server The server that succeeded.
     */
    public void reportSuccess(ServerNode server) {
        if (server == null || server.isDownState) {
            return;
        }

        lock.lock();
        try {
            if (server.fails > 0) {
                server.fails = 0;
            }
        } finally {
            lock.unlock();
        }
    }

    public void printServerStates() {
        lock.lock();
        try {
            System.out.println("--- Current Server States ---");
            servers.forEach(System.out::println);
            System.out.println("-----------------------------");
        } finally {
            lock.unlock();
        }
    }
}
