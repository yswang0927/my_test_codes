package loadbalancer;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Test {
    public static void main(String[] args) throws InterruptedException {
        // Configuration: 3 servers with different weights, max_fails, and fail_timeout
        List<ServerNode> servers = Arrays.asList(
                new ServerNode<String>("primary1:8080", 5, 1, 5), // High weight, 2 max fails, 10s timeout
                new ServerNode<String>("primary2:8081", 2, 1, 5), // Medium weight
                new ServerNode<String>("backup1:9090", 0, 1, 1, 5, true, false),
                new ServerNode<String>("down-server:9999", 0, 1, 2, 10, true, true)
        );

        WeightedRoundRobinLoadBalancer lb = new WeightedRoundRobinLoadBalancer(servers);

        System.out.println("🚀 Simulating requests...\n");
        lb.printServerStates();

        // --- Phase 1: Primary servers are healthy ---
        System.out.println("\n--- Phase 1: Primary servers are healthy ---");
        for (int i = 1; i <= 3; i++) {
            System.out.printf("Request #%d: ", i);
            ServerNode s = lb.selectServer();
            System.out.printf("Selected %s%n", s.endpoint);
        }
        lb.printServerStates();

        // --- Phase 2: Fail all primary servers ---
        System.out.println("\n--- Phase 2: Failing all primary servers ---");
        System.out.print("Request #4: Selected primary1:8080. ");
        lb.reportFailure(servers.get(0)); // Fail primary1

        System.out.print("Request #5: Selected primary2:8081. ");
        lb.reportFailure(servers.get(1)); // Fail primary2

        lb.printServerStates();

        // --- Phase 3: All primaries are down, should select backup ---
        System.out.println("\n--- Phase 3: Fallback to backup server ---");
        for (int i = 6; i <= 8; i++) {
            System.out.printf("Request #%d: ", i);
            ServerNode s = lb.selectServer();
            System.out.printf("Selected %s%n", s.endpoint);
        }
        lb.printServerStates();

        // --- Phase 4: Wait for primaries to recover ---
        System.out.println("\n--- Phase 4: Waiting for primary servers to recover ---");
        TimeUnit.SECONDS.sleep(6);

        System.out.printf("Request #9: ");
        ServerNode s = lb.selectServer(); // Should select a recovered primary
        System.out.printf("Selected %s%n", s.endpoint);
        lb.printServerStates();
    }
}