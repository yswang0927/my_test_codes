package dag2;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 一个支持条件路径和数据传递的健壮的DAG工作流执行引擎。
 * 修复了条件分支跳过时导致的死锁问题。
 */
public class DAGExecutor {
    // 存储节点定义
    private final Map<String, WorkflowNode> nodes = new ConcurrentHashMap<>();
    // 邻接表，存储所有可能的路径
    private final Map<String, List<String>> adjacencyList = new ConcurrentHashMap<>();
    // 入度表，使用原子类保证线程安全
    private final Map<String, AtomicInteger> inDegree = new ConcurrentHashMap<>();
    // 前置依赖表，用于运行时聚合上游节点的输出数据
    private final Map<String, List<String>> predecessors = new ConcurrentHashMap<>();

    // --- 以下为每次执行时需要重置的状态 ---
    // 存储每个节点的输出数据
    private Map<String, Map<String, Object>> nodeOutputs;
    // 使用原子计数器跟踪已完成的任务总数（包括执行和跳过）
    private AtomicInteger completedTasksCounter;
    // 跟踪所有被跳过的节点，防止在复杂的图中重复处理
    private Set<String> skippedNodes;


    public void addNode(WorkflowNode node) {
        nodes.put(node.getId(), node);
        adjacencyList.put(node.getId(), new CopyOnWriteArrayList<>()); // 使用线程安全的List
        predecessors.put(node.getId(), new CopyOnWriteArrayList<>());
        inDegree.put(node.getId(), new AtomicInteger(0));
    }

    public void addDependency(String from, String to) {
        adjacencyList.get(from).add(to);
        predecessors.get(to).add(from);
        inDegree.get(to).incrementAndGet();
    }

    /**
     * 执行工作流。
     *
     * @param initialData 工作流的初始输入数据
     */
    public void executeWorkflow(Map<String, Object> initialData) throws InterruptedException {
        // 1. 初始化或重置每次执行的状态变量
        this.nodeOutputs = new ConcurrentHashMap<>();
        this.completedTasksCounter = new AtomicInteger(0);
        this.skippedNodes = ConcurrentHashMap.newKeySet();
        this.nodeOutputs.put("__start__", initialData);

        // 重新计算所有节点的入度，以保证执行器实例可重用
        final Map<String, AtomicInteger> currentInDegree = new ConcurrentHashMap<>();
        nodes.keySet().forEach(nodeId -> currentInDegree.put(nodeId, new AtomicInteger(0)));
        adjacencyList.forEach((from, toList) -> {
            for (String to : toList) {
                currentInDegree.get(to).incrementAndGet();
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        ExecutorCompletionService<ExecutionResult> completionService = new ExecutorCompletionService<>(executor);

        Queue<String> readyQueue = new ConcurrentLinkedQueue<>();

        // 2. 找到所有初始入度为0的节点，加入就绪队列
        currentInDegree.forEach((nodeId, degree) -> {
            if (degree.get() == 0) {
                readyQueue.offer(nodeId);
            }
        });

        if (readyQueue.isEmpty() && !nodes.isEmpty()) {
            System.err.println("🛑 Error: No nodes with zero in-degree found. Possible circular dependency in the graph.");
            executor.shutdown();
            return;
        }

        int submittedTasks = 0;
        while (!readyQueue.isEmpty()) {
            String nodeId = readyQueue.poll();
            submitTask(nodeId, completionService, currentInDegree);
            submittedTasks++;
        }

        // 3. 主循环：等待任务完成，并根据结果推进工作流
        while (completedTasksCounter.get() < nodes.size()) {
            try {
                // 使用带超时的poll，避免在逻辑错误时无限期阻塞
                Future<ExecutionResult> future = completionService.poll(5, TimeUnit.SECONDS);
                if (future == null) {
                    if (submittedTasks == completedTasksCounter.get()) {
                        System.err.println("🛑 Workflow stalled. Completed " + completedTasksCounter.get() + "/" + nodes.size() + " tasks. Check for cycles or logic errors.");
                        break;
                    }
                    continue; // 如果还有任务在运行，继续等待
                }

                ExecutionResult result = future.get();
                String finishedNodeId = result.getSourceNodeId(); // FIX: 直接从结果获取ID，安全可靠

                if (!result.isSuccess()) {
                    System.err.println("💀 Task " + finishedNodeId + " failed. Aborting workflow.");
                    break; // 简化处理，直接中止
                }

                // 4. 处理成功的任务
                System.out.println("✅ Task '" + nodes.get(finishedNodeId).getName() + "' completed.");
                completedTasksCounter.incrementAndGet();
                nodeOutputs.put(finishedNodeId, result.getOutputData());

                List<String> allDependents = adjacencyList.getOrDefault(finishedNodeId, List.of());
                List<String> nodesToActivate = result.getNextNodeIdsToActivate();

                // 如果节点未指定激活路径，则默认激活所有下游
                if (nodesToActivate.isEmpty()) {
                    nodesToActivate = allDependents;
                }

                // 5. **核心修复逻辑**: 处理下游依赖
                for (String dependentId : allDependents) {
                    if (nodesToActivate.contains(dependentId)) {
                        // 路径被激活：正常处理入度，如果为0则加入就绪队列
                        if (currentInDegree.get(dependentId).decrementAndGet() == 0) {
                            readyQueue.offer(dependentId);
                        }
                    } else {
                        // 路径被跳过：启动“跳过”传播
                        propagateSkip(dependentId, readyQueue, currentInDegree);
                    }
                }

                // 6. 提交新一批就绪的任务
                while (!readyQueue.isEmpty()) {
                    String nodeId = readyQueue.poll();
                    submitTask(nodeId, completionService, currentInDegree);
                    submittedTasks++;
                }

            } catch (Exception e) {
                System.err.println("An unexpected error occurred during workflow execution: " + e.getMessage());
                e.printStackTrace();
                break;
            }
        }

        if (completedTasksCounter.get() == nodes.size()) {
            System.out.println("🎉 DAG workflow execution completed successfully.");
        } else {
            System.out.println("🏁 Workflow finished. Completed " + completedTasksCounter.get() + " out of " + nodes.size() + " total tasks.");
        }

        executor.shutdownNow(); // 确保所有线程被关闭
    }

    /**
     * 递归地传播“跳过”状态。这是解决死锁的关键。
     * 当一个节点被其上游跳过时，它本身也必须被视为“跳过”，并将其完成状态向下游传播。
     */
    private void propagateSkip(String nodeIdToSkip, Queue<String> readyQueue, Map<String, AtomicInteger> currentInDegree) {
        if (skippedNodes.contains(nodeIdToSkip) || nodeOutputs.containsKey(nodeIdToSkip)) {
            return; // 如果节点已经被跳过或已经执行完成，则直接返回
        }

        // 关键：即使是跳过的节点，也要先将它的入度减1
        if (currentInDegree.get(nodeIdToSkip).decrementAndGet() == 0) {
            System.out.println("⏭️ Skipping node '" + nodes.get(nodeIdToSkip).getName() + "' as its dependency path was not activated.");
            skippedNodes.add(nodeIdToSkip);
            completedTasksCounter.incrementAndGet();

            // 将跳过状态继续向下游传播
            List<String> dependents = adjacencyList.getOrDefault(nodeIdToSkip, List.of());
            for (String dependentId : dependents) {
                propagateSkip(dependentId, readyQueue, currentInDegree);
            }
        }
    }

    /**
     * 聚合上游输入数据并提交任务到线程池。
     */
    private void submitTask(String nodeId, ExecutorCompletionService<ExecutionResult> cs, Map<String, AtomicInteger> currentInDegree) {
        WorkflowNode node = nodes.get(nodeId);
        Map<String, Object> inputs = new HashMap<>();
        List<String> predNodeIds = predecessors.getOrDefault(nodeId, List.of());

        if (predNodeIds.isEmpty()) { // 如果是根节点
            inputs.putAll(nodeOutputs.getOrDefault("__start__", Map.of()));
        } else {
            for (String predId : predNodeIds) {
                // 只聚合已完成节点的输出
                if (nodeOutputs.containsKey(predId)) {
                    inputs.putAll(nodeOutputs.get(predId));
                }
            }
        }

        System.out.println("📨 Submitting task: '" + node.getName() + "' with inputs: " + inputs);
        cs.submit(() -> node.call(inputs));
    }

    public static void main(String[] args) throws InterruptedException {
        // 为了方便运行，这里包含了所有依赖的类定义
        // 在实际项目中，这些都应该是独立的 .java 文件

        // --- main 方法和之前一样，用于测试 ---
        DAGExecutor executor = new DAGExecutor();

        WorkflowNode start = new SimpleTaskNode("1", "Start Generating Value");
        WorkflowNode check = new IfConditionNode("2", "Check Value (>10)", "3", "4");
        WorkflowNode truePath = new SimpleTaskNode("3", "Execute True Path");
        WorkflowNode falsePath = new SimpleTaskNode("4", "Execute False Path");
        WorkflowNode end = new SimpleTaskNode("5", "End");

        executor.addNode(start);
        executor.addNode(check);
        executor.addNode(truePath);
        executor.addNode(falsePath);
        executor.addNode(end);

        executor.addDependency("1", "2");
        executor.addDependency("2", "3");
        executor.addDependency("2", "4");
        executor.addDependency("3", "5");
        executor.addDependency("4", "5");

        System.out.println("--- Running with value that triggers TRUE path (15 > 10) ---");
        executor.executeWorkflow(Map.of("value", 15));

        System.out.println("\n\n--- Running with value that triggers FALSE path (5 <= 10) ---");
        executor.executeWorkflow(Map.of("value", 5));
    }
}
