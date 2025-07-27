package loadbalancer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public abstract class ServerGroup {

    protected final List<ServerNode> mainServers = new ArrayList<>();
    protected final List<ServerNode> backupServers = new ArrayList<>();

    // 主服务器总权重
    protected int mainTotalWeight = 0;
    // 备份服务器总权重
    protected int backupTotalWeight = 0;
    // 是否已经切换到备份服务器
    protected volatile boolean usingBackup = false;

    public ServerGroup() {
    }

    public ServerGroup(Collection<ServerNode> serverList) {
        if (serverList != null) {
            for (ServerNode serverNode : serverList) {
                addServer(serverNode);
            }
        }
    }

    public void addServer(ServerNode server) {
        if (server == null || server.isDown()) {
            return;
        }

        if (server.isBackup) {
            backupServers.add(server);
            backupTotalWeight += server.weight;
        } else {
            mainServers.add(server);
            mainTotalWeight += server.weight;
        }
    }

    public abstract ServerNode getNextServer();

}
