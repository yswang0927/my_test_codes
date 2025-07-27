package dag1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * 使用拓扑排序来确定执行顺序，并使用 ExecutorService 来并行运行任务。
 *
 * 清晰的图结构：使用了邻接表 (adjacencyList) 和入度表 (inDegree) 来表示DAG，
 * 这是处理此类问题的标准且高效的数据结构。
 *
 * 正确的拓扑排序逻辑：通过维护一个“就绪队列” (readyQueue)，从入度为0的节点开始执行，
 * 并在任务完成后更新其后续节点的入度，这个核心算法是完全正确的。
 *
 */
public class DAGExecutor {
    private final Map<String, WorkflowNode> nodes = new HashMap<>();
    private final Map<String, List<String>> adjacencyList = new HashMap<>();
    private final Map<String, Integer> inDegree = new HashMap<>();

    public void addNode(WorkflowNode node) {
        nodes.put(node.getId(), node);
        adjacencyList.put(node.getId(), new ArrayList<>());
        inDegree.put(node.getId(), 0);
    }

    public void addDependency(String from, String to) {
        adjacencyList.get(from).add(to);
        inDegree.put(to, inDegree.get(to) + 1);
    }

    public void executeWorkflow() throws InterruptedException {
        System.out.println("Starting DAG workflow execution...");
        ExecutorService executor = Executors.newFixedThreadPool(4);
        Queue<String> readyQueue = new LinkedList<>();
        Map<String, Future<Boolean>> runningTasks = new HashMap<>();

        // Initialize readyQueue with nodes having zero dependencies (in-degree == 0)
        for (Map.Entry<String, Integer> entry : inDegree.entrySet()) {
            if (entry.getValue() == 0) {
                readyQueue.offer(entry.getKey());
            }
        }

        while (!readyQueue.isEmpty() || !runningTasks.isEmpty()) {
            while (!readyQueue.isEmpty()) {
                String nodeId = readyQueue.poll(); // Initialize nodeId properly here
                WorkflowNode node = nodes.get(nodeId);
                System.out.println("Submitting task: " + node.getId());
                Future<Boolean> future = executor.submit(node);
                runningTasks.put(nodeId, future);
            }

            Iterator<Map.Entry<String, Future<Boolean>>> iterator = runningTasks.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Future<Boolean>> entry = iterator.next();
                String nodeId = entry.getKey(); // Ensure nodeId is accessible here
                Future<Boolean> future = entry.getValue();

                if (future.isDone()) {
                    iterator.remove();
                    try {
                        if (future.get()) {
                            System.out.println("Task " + nodeId + " completed successfully.");
                            // Add dependent tasks to readyQueue if all their dependencies are met
                            for (String dependent : adjacencyList.getOrDefault(nodeId, new ArrayList<>())) {
                                inDegree.put(dependent, inDegree.get(dependent) - 1);
                                if (inDegree.get(dependent) == 0) {
                                    readyQueue.offer(dependent);
                                }
                            }
                        } else {
                            System.out.println("Task " + nodeId + " execution failed.");
                        }
                    } catch (ExecutionException e) {
                        System.err.println("Error executing task " + nodeId + ": " + e.getMessage());
                    }
                }
            }
        }

        executor.shutdown();
        System.out.println("DAG workflow execution completed.");
    }

    /**
     * 可改进之处 (Areas for Improvement) 🧐
     * CPU空转/忙等待 (Busy-Waiting)：
     *
     * 问题：在主执行循环 while (!readyQueue.isEmpty() || !runningTasks.isEmpty()) 中，
     * 内部的 while (iterator.hasNext()) 会不断地、无间断地轮询 future.isDone()。
     * 当任务正在执行且尚未完成时，这个循环会消耗大量CPU资源，造成不必要的性能浪费。
     *
     * 影响：当任务执行时间较长或任务数量较多时，主线程会一直处于高CPU占用状态。
     *
     * 容错性不足 (Limited Fault Tolerance)：
     * 问题：当前代码只打印了任务失败的日志。如果一个节点失败，依赖它的后续节点将永远不会被执行（因为它们的入度不会减为0），
     * 但整个工作流并不会明确地标记为失败，其他并行的分支会继续执行。没有重试机制。
     *
     * 目标：需要一个更明确的失败处理策略（例如，快速失败、收集所有错误）和重试能力。
     *
     * 潜在的并发问题 (Potential Concurrency Issues)：
     * 问题：inDegree 这个 HashMap 被多个线程访问。虽然在你当前的设计中，只有主线程在检查完 Future 后修改它，
     * 逻辑上是安全的。但如果未来逻辑变得更复杂（例如，任务自己可以触发其他任务），HashMap 并不是线程安全的，可能会引发问题。
     * 使用线程安全的数据结构是更稳健的做法。
     *
     * 缺少循环依赖检测 (No Cycle Detection)：
     * 问题：如果工作流中存在循环（例如 A -> B -> C -> A），那么循环中的节点的入度永远不会变为0。
     * 执行引擎会启动，执行到循环之前的节点，然后静默地结束，而不会报告有节点未被执行。
     *
     * 影响：这是一种难以发现的逻辑错误，引擎应该能检测并明确报告出来。
     *
     * 节点定义的冗余 (Redundancy in Node Definition)：
     * WorkflowNode 的构造函数接受一个 dependencies 列表，但在 main 方法中，
     * 依赖关系是通过 executor.addDependency() 方法独立添加的。
     * 这造成了API的困惑和数据冗余。
     */
    public static void main(String[] args) throws InterruptedException {
        DAGExecutor executor = new DAGExecutor();

        WorkflowNode node1 = new WorkflowNode("1", "Start", List.of());
        WorkflowNode node2 = new WorkflowNode("2", "Fetch Data", List.of("1"));
        WorkflowNode node3 = new WorkflowNode("3", "Check Condition", List.of("2"));
        WorkflowNode node4 = new WorkflowNode("4", "Send Email", List.of("3"));

        executor.addNode(node1);
        executor.addNode(node2);
        executor.addNode(node3);
        executor.addNode(node4);

        executor.addDependency("1", "2");
        executor.addDependency("2", "3");
        executor.addDependency("3", "4");

        executor.executeWorkflow();
    }
}