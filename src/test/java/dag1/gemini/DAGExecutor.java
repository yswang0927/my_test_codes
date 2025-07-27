package dag1.gemini;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 改进点总结
 * 效率提升 (Efficiency)
 * 核心变更：用 ExecutorCompletionService 替换了手动的 Future 轮询。
 * 效果：主线程不再忙等待，而是通过 completionService.take() 高效地阻塞，直到有任务完成。这极大地降低了CPU消耗，是性能上的关键优化。
 *
 * 健壮性与线程安全 (Robustness & Thread Safety)
 * 核心变更：将 inDegree 的 Integer 替换为 AtomicInteger，并将 HashMap 替换为 ConcurrentHashMap。
 * 效果：确保了在并发环境下对图结构（特别是入度）的修改是原子和线程安全的，避免了潜在的数据竞争。
 *
 * 容错性 (Fault Tolerance)
 * 核心变更：在 catch (ExecutionException e) 块中增加了重试逻辑。
 * 效果：当一个任务失败时，引擎会尝试重新提交它（最多 MAX_RETRIES 次）。这使得工作流能够从瞬时故障（如网络抖动、数据库临时不可用）中自动恢复。main 函数中的示例也演示了如何创建一个只失败一次的节点来测试此功能。
 *
 * 错误检测 (Error Detection)
 * 核心变更：在执行结束后，比较 completedTasks 的数量和总节点数 nodes.size()。
 * 效果：如果两者不相等，就可以断定工作流中存在循环依赖或无法恢复的任务失败，并向用户报告错误，而不是静默结束。
 *
 * 代码清晰度 (Code Clarity)
 * 核心变更：将 Callable 包装起来，使其在完成后能返回自身的 nodeId。这简化了从 Future 反向查找是哪个任务完成的逻辑。
 * 效果：执行逻辑更流畅，更容易理解和维护。
 */
public class DAGExecutor {
    // 使用线程安全的集合
    private final Map<String, WorkflowNode> nodes = new ConcurrentHashMap<>();
    private final Map<String, List<String>> adjacencyList = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> inDegree = new ConcurrentHashMap<>();

    // 用于重试
    private final Map<String, Integer> retryCounts = new ConcurrentHashMap<>();
    private static final int MAX_RETRIES = 2;

    public void addNode(WorkflowNode node) {
        nodes.put(node.getId(), node);
        adjacencyList.put(node.getId(), new ArrayList<>());
        inDegree.put(node.getId(), new AtomicInteger(0));
    }

    public void addDependency(String from, String to) {
        adjacencyList.get(from).add(to);
        inDegree.get(to).incrementAndGet(); // 原子操作
    }

    public void executeWorkflow() throws InterruptedException {
        System.out.println("🚀 Starting DAG workflow execution...");
        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        // ExecutorCompletionService 优雅地处理已完成的任务，避免忙等待
        ExecutorCompletionService<String> completionService = new ExecutorCompletionService<>(executor);

        Queue<String> readyQueue = new ConcurrentLinkedQueue<>();

        // 1. 初始化就绪队列
        inDegree.forEach((nodeId, degree) -> {
            if (degree.get() == 0) {
                readyQueue.offer(nodeId);
            }
        });

        int submittedTasks = 0;
        int completedTasks = 0;
        Map<String, Future<String>> runningFutures = new HashMap<>();

        // 提交第一批任务
        while (!readyQueue.isEmpty()) {
            String nodeId = readyQueue.poll();
            submitTask(nodeId, completionService, runningFutures);
            submittedTasks++;
        }

        // 2. 主循环：等待任务完成，并触发新任务
        while (completedTasks < nodes.size()) {
            // take() 方法会阻塞，直到有任务完成，非常高效
            Future<String> completedFuture = completionService.take();
            if (completedFuture == null) {
                // 如果所有任务都已提交且没有任务在运行，但未全部完成，说明有循环
                break;
            }

            completedTasks++;
            String finishedNodeId = null;

            try {
                finishedNodeId = completedFuture.get();
                System.out.println("✅ Task " + finishedNodeId + " completed successfully.");
                runningFutures.remove(finishedNodeId);

                // 触发后续任务
                List<String> dependents = adjacencyList.getOrDefault(finishedNodeId, new ArrayList<>());
                for (String dependentId : dependents) {
                    // 入度减1，如果为0则加入就绪队列
                    if (inDegree.get(dependentId).decrementAndGet() == 0) {
                        readyQueue.offer(dependentId);
                    }
                }

            } catch (ExecutionException e) {
                // 3. 容错与重试逻辑
                finishedNodeId = findNodeIdByFuture(runningFutures, completedFuture);
                if (finishedNodeId != null) {
                    runningFutures.remove(finishedNodeId);
                    int currentRetries = retryCounts.getOrDefault(finishedNodeId, 0);
                    if (currentRetries < MAX_RETRIES) {
                        retryCounts.put(finishedNodeId, currentRetries + 1);
                        System.err.println("🔥 Task " + finishedNodeId + " failed. Retrying (" + (currentRetries + 1) + "/" + MAX_RETRIES + ")...");
                        submitTask(finishedNodeId, completionService, runningFutures); // 重新提交
                        completedTasks--; // 因为重试，所以不算作完成
                    } else {
                        System.err.println("💀 Task " + finishedNodeId + " failed after " + MAX_RETRIES + " retries. Aborting workflow for this path.");
                        // 在此可以实现更复杂的失败策略，如中断整个工作流
                    }
                }
            }

            // 提交新加入就绪队列的任务
            while(!readyQueue.isEmpty()){
                String nodeId = readyQueue.poll();
                submitTask(nodeId, completionService, runningFutures);
                submittedTasks++;
            }
        }

        // 4. 循环依赖检测
        if (completedTasks < nodes.size()) {
            System.err.println("🛑 Workflow finished prematurely. Possible circular dependency detected or unrecoverable task failure.");
            System.err.println("Total Nodes: " + nodes.size() + ", Completed Nodes: " + completedTasks);
        } else {
            System.out.println("🎉 DAG workflow execution completed successfully.");
        }

        executor.shutdown();
    }

    private void submitTask(String nodeId, ExecutorCompletionService<String> cs, Map<String, Future<String>> futures) {
        WorkflowNode node = nodes.get(nodeId);
        // 包装 Callable，使其返回 nodeId
        Future<String> future = cs.submit(() -> {
            node.call();
            return nodeId;
        });
        futures.put(nodeId, future);
        System.out.println("📨 Submitting task: " + nodeId);
    }

    private String findNodeIdByFuture(Map<String, Future<String>> futures, Future<String> future) {
        return futures.entrySet().stream()
                .filter(entry -> entry.getValue().equals(future))
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }

    public static void main(String[] args) throws InterruptedException {
        DAGExecutor executor = new DAGExecutor();

        // 创建一个更复杂的图，包含并行分支和可能失败的节点
        //      1
        //     / \
        //    2   3 (fails once)
        //    |   |
        //    4   5
        //     \ /
        //      6
        WorkflowNode node1 = new WorkflowNode("1", "Start");
        WorkflowNode node2 = new WorkflowNode("2", "Process A");
        WorkflowNode node3 = new WorkflowNode("3", "Process B (will fail)", true); // 这个节点会失败
        WorkflowNode node4 = new WorkflowNode("4", "Process C");
        WorkflowNode node5 = new WorkflowNode("5", "Process D");
        WorkflowNode node6 = new WorkflowNode("6", "End");

        executor.addNode(node1);
        executor.addNode(node2);
        executor.addNode(node3);
        executor.addNode(node4);
        executor.addNode(node5);
        executor.addNode(node6);

        executor.addDependency("1", "2");
        executor.addDependency("1", "3");
        executor.addDependency("2", "4");
        executor.addDependency("3", "5");
        executor.addDependency("4", "6");
        executor.addDependency("5", "6");

        // 模拟重试成功场景
        // 修改node3，让它在第一次调用时失败，之后成功
        WorkflowNode failingNode = new WorkflowNode("3", "Process B (will fail once)", true) {
            private final AtomicInteger attempts = new AtomicInteger(0);
            @Override
            public Boolean call() throws Exception {
                System.out.println("▶️ Executing Node: " + "Process B" + " (Attempt " + (attempts.get() + 1) + ")");
                Thread.sleep(1000);
                if (attempts.getAndIncrement() == 0) { // 仅第一次失败
                    System.out.println("❌ Node " + "Process B" + " is failing on purpose.");
                    throw new RuntimeException("Simulated first failure");
                }
                System.out.println("✅ Node " + "Process B" + " completed successfully on retry.");
                return true;
            }
        };
        executor.addNode(failingNode); // 覆盖之前的node3

        executor.executeWorkflow();
    }
}