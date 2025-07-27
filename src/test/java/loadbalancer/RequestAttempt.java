package loadbalancer;

import java.util.HashSet;
import java.util.Set;

/**
 * Represents the state of a single upstream request attempt, including retries.
 * Mirrors Nginx's ngx_http_upstream_rr_peer_data_t.
 */
public class RequestAttempt {
    private final Set<ServerNode> triedPeers;
    private ServerNode currentPeer;

    public RequestAttempt() {
        this.triedPeers = new HashSet<>();
    }

    public Set<ServerNode> getTriedPeers() {
        return triedPeers;
    }

    public ServerNode getCurrentPeer() {
        return currentPeer;
    }

    public void setCurrentPeer(ServerNode currentPeer) {
        this.currentPeer = currentPeer;
        if (currentPeer != null) {
            this.triedPeers.add(currentPeer);
        }
    }
}
