package loadbalancer;

public class Test2 {

    public static void main(String[] args) {
        WeightedRoundRobbinServerGroup serverGroup = new WeightedRoundRobbinServerGroup();

        // 添加主服务器
        serverGroup.addServer(new ServerNode("server1.example.com", 5, 0, 1, 10, false, false));
        serverGroup.addServer(new ServerNode("server2.example.com", 2, 0, 1, 10, false, false));
        serverGroup.addServer(new ServerNode("backup.example.com", 2, 0, 1, 10, true, false));

        // 模拟请求，选择服务器
        for (int i = 1; i <= 10; i++) {
            final int requestNum = i;
            new Thread(() -> {
                ServerNode server = serverGroup.getNextServer();
                if (server != null) {
                    System.out.println("Request " + requestNum + " is sent to " + (server.isBackup ? "backup " : "") + server.endpoint);
                    // 模拟前10个请求失败，测试故障转移
                    boolean requestFailed = requestNum < 5;

                    // 模拟处理时间
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    serverGroup.freeServer(server, requestFailed);
                } else {
                    System.out.println("No available server for request " + requestNum);
                }
            }).start();
        }
    }

    public static void main2(String[] args) {
        WeightedRoundRobbinServerGroup serverGroup = new WeightedRoundRobbinServerGroup();

        // 添加主服务器
        serverGroup.addServer(new ServerNode("server1.example.com", 3, 10, 3, 10000, false, false));
        serverGroup.addServer(new ServerNode("server2.example.com", 2, 10, 3, 10000, false, false));

        // 添加备份服务器
        serverGroup.addServer(new ServerNode("backup1.example.com", 1, 5, 2, 10000, true, false));
        serverGroup.addServer(new ServerNode("backup2.example.com", 1, 5, 2, 10000, true, false));

        // 模拟请求，选择服务器
        for (int i = 0; i < 20; i++) {
            final int requestNum = i;
            // 使用线程模拟并发请求
            new Thread(() -> {
                ServerNode server = serverGroup.getNextServer();
                if (server != null) {
                    System.out.println("Request " + requestNum + " is sent to " + (server.isBackup ? "backup " : "") + server.endpoint);
                    // 模拟前10个请求失败，测试故障转移
                    boolean requestFailed = requestNum < 10;

                    // 模拟处理时间
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    serverGroup.freeServer(server, requestFailed);
                } else {
                    System.out.println("No available server for request " + requestNum);
                }
            }).start();

            // 控制并发度
            if (i % 5 == 0) {
                try {
                    Thread.sleep(200);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        // 等待所有线程完成
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
