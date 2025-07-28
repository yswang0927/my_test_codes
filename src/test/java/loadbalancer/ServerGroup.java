package loadbalancer;

public interface ServerGroup {

    void addServer(ServerNode server);

    ServerNode getNextServer();

}
