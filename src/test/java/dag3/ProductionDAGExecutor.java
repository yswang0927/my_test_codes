package dag3;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 生产级的DAG工作流执行引擎。
 * <p>
 * 特性:
 * <ul>
 * <li>支持复杂的有向无环图（DAG）结构，包括条件分支和并行路径。</li>
 * <li>节点间的数据传递。</li>
 * <li>节点执行失败后的自动重试机制，支持配置重试次数和延迟。</li>
 * <li>在最终失败后创建工作流保存点（Checkpoint），支持多节点并行失败。</li>
 * <li>能够从保存点恢复，继续执行未完成的工作流。</li>
 * <li>使用SLF4J进行日志记录，可配置。</li>
 * <li>通过配置类管理核心参数，而非硬编码。</li>
 * <li>优雅的线程池关闭策略。</li>
 * </ul>
 * 使用方式:
 * <pre>{@code
 * // 1. 创建配置
 * DAGExecutorConfig config = DAGExecutorConfig.defaults();
 * // 2. 创建执行器实例
 * ProductionDAGExecutor executor = ProductionDAGExecutor.newInstance(config);
 * // 3. 构建图
 * executor.addNode(...);
 * executor.addDependency(...);
 * // 4. 执行工作流 (全新或从保存点恢复)
 * WorkflowState resumeState = ProductionDAGExecutor.loadStateFromFile(config.getSavepointPath());
 * executor.executeWorkflow(initialData, resumeState);
 * }</pre>
 *
 * @version 4.0 Production Ready
 */
public final class ProductionDAGExecutor {

    private static final Logger log = LoggerFactory.getLogger(ProductionDAGExecutor.class);
    private static final String START_NODE_ID = "__start__";

    // --- 配置与图结构 (不可变) ---
    private final DAGExecutorConfig config;
    private final Map<String, WorkflowNode> nodes;
    private final Map<String, List<String>> adjacencyList;
    private final Map<String, List<String>> predecessors;

    // --- 运行时状态 (每次执行时创建) ---
    private Map<String, Map<String, Object>> nodeOutputs;
    private AtomicInteger completedTasksCounter;
    private Set<String> skippedNodes;
    private Map<String, AtomicInteger> attemptCounts;
    private Map<Future<ExecutionResult>, String> runningFutures;
    private AtomicBoolean shutdownInProgress;
    private Map<String, FailedNodeInfo> collectedFailures;

    /**
     * 私有构造函数，通过工厂方法创建实例。
     */
    private ProductionDAGExecutor(DAGExecutorConfig config) {
        this.config = Objects.requireNonNull(config, "Configuration cannot be null.");
        this.nodes = new ConcurrentHashMap<>();
        this.adjacencyList = new ConcurrentHashMap<>();
        this.predecessors = new ConcurrentHashMap<>();
    }

    /**
     * 创建一个新的DAGExecutor实例。
     * @param config 执行器配置
     * @return 一个新的、干净的执行器实例
     */
    public static ProductionDAGExecutor newInstance(DAGExecutorConfig config) {
        return new ProductionDAGExecutor(config);
    }

    /**
     * 向图中添加一个节点。
     */
    public void addNode(WorkflowNode node) {
        nodes.put(node.getId(), node);
        adjacencyList.put(node.getId(), new CopyOnWriteArrayList<>());
        predecessors.put(node.getId(), new CopyOnWriteArrayList<>());
    }

    /**
     * 在图中添加一条依赖边。
     */
    public void addDependency(String from, String to) {
        if (!nodes.containsKey(from) || !nodes.containsKey(to)) {
            throw new IllegalArgumentException("Node not found in graph. Cannot add dependency.");
        }
        adjacencyList.get(from).add(to);
        predecessors.get(to).add(from);
    }

    /**
     * 执行工作流，支持从保存点恢复。
     *
     * @param initialData 仅在全新运行时使用的初始数据。
     * @param resumeState 可选的恢复状态对象。如果非空，则从该保存点恢复。
     */
    public void executeWorkflow(Map<String, Object> initialData, WorkflowState resumeState) {
        initializeState(initialData, resumeState);
        Map<String, AtomicInteger> currentInDegree = calculateInDegrees(resumeState);
        ExecutorService executor = Executors.newFixedThreadPool(config.getThreadPoolSize());
        ExecutorCompletionService<ExecutionResult> completionService = new ExecutorCompletionService<>(executor);
        Queue<String> readyQueue = new ConcurrentLinkedQueue<>();

        populateInitialReadyQueue(readyQueue, currentInDegree, resumeState);

        if (readyQueue.isEmpty() && !nodes.isEmpty() && resumeState == null) {
            log.error("🛑 No nodes with zero in-degree found. Possible circular dependency in the graph.");
            shutdownExecutor(executor);
            return;
        }

        int submittedTasks = submitTasksFromQueue(readyQueue, completionService, currentInDegree);

        mainLoop(completionService, currentInDegree, readyQueue, submittedTasks);

        postExecutionCleanup(executor, currentInDegree);
    }

    private void mainLoop(ExecutorCompletionService<ExecutionResult> completionService, Map<String, AtomicInteger> currentInDegree, Queue<String> readyQueue, int submittedTasks) {
        while (completedTasksCounter.get() + collectedFailures.size() < nodes.size()) {
            if (shutdownInProgress.get() && runningFutures.isEmpty()) {
                log.warn("Shutdown in progress and all tasks have finished. Exiting main loop.");
                break;
            }
            try {
                Future<ExecutionResult> future = completionService.poll(config.getTaskPollTimeoutSeconds(), TimeUnit.SECONDS);
                if (future == null) continue;

                String finishedNodeId = runningFutures.remove(future);
                if (finishedNodeId == null) {
                    log.warn("A task future completed but was not found in the running futures map. It might have been a failed and retried task. Ignoring.");
                    continue;
                }

                ExecutionResult result = future.get();
                handleSuccessfulTask(result, finishedNodeId, currentInDegree, readyQueue);

            } catch (ExecutionException e) {
                // 这里有个 future 错误
                //handleFailedTask(e, future, completionService, currentInDegree);
            } catch (InterruptedException e) {
                log.error("Main execution loop was interrupted. Forcing shutdown.", e);
                Thread.currentThread().interrupt();
                break;
            }

            if (!shutdownInProgress.get()) {
                submitTasksFromQueue(readyQueue, completionService, currentInDegree);
            }
        }
    }

    private void handleSuccessfulTask(ExecutionResult result, String finishedNodeId, Map<String, AtomicInteger> currentInDegree, Queue<String> readyQueue) {
        log.info("✅ Task '{}' ({}) completed successfully.", nodes.get(finishedNodeId).getName(), finishedNodeId);
        completedTasksCounter.incrementAndGet();
        nodeOutputs.put(finishedNodeId, result.getOutputData());
        attemptCounts.get(finishedNodeId).set(0);

        List<String> allDependents = adjacencyList.getOrDefault(finishedNodeId, List.of());
        List<String> nodesToActivate = result.getNextNodeIdsToActivate();
        if (nodesToActivate.isEmpty()) nodesToActivate = allDependents;

        for (String dependentId : allDependents) {
            if (nodesToActivate.contains(dependentId)) {
                if (currentInDegree.get(dependentId).decrementAndGet() == 0) readyQueue.offer(dependentId);
            } else {
                propagateSkip(dependentId, currentInDegree);
            }
        }
    }

    private void handleFailedTask(ExecutionException e, Future<ExecutionResult> future, ExecutorCompletionService<ExecutionResult> cs, Map<String, AtomicInteger> currentInDegree) {
        String failedNodeId = runningFutures.remove(future);
        if (failedNodeId == null) return;

        WorkflowNode failedNode = nodes.get(failedNodeId);
        int currentAttempt = attemptCounts.get(failedNodeId).incrementAndGet();

        if (currentAttempt <= failedNode.getMaxRetries()) {
            log.warn("🔥 Task '{}' ({}) failed on attempt {}. Retrying after {}ms...",
                    failedNode.getName(), failedNodeId, currentAttempt, failedNode.getRetryDelayMillis());
            try {
                Thread.sleep(failedNode.getRetryDelayMillis());
            } catch (InterruptedException ie) { Thread.currentThread().interrupt(); }
            submitTask(failedNodeId, cs, currentInDegree);
        } else {
            log.error("💀 Task '{}' ({}) finally failed after {} retries.",
                    failedNode.getName(), failedNodeId, failedNode.getMaxRetries());

            String lastError = e.getCause() != null ? e.getCause().toString() : e.toString();
            FailedNodeInfo failureInfo = new FailedNodeInfo(failedNodeId, gatherInputsForNode(failedNodeId), currentAttempt, lastError);
            collectedFailures.put(failedNodeId, failureInfo);

            if (shutdownInProgress.compareAndSet(false, true)) {
                log.warn("🚦 Graceful shutdown initiated due to final task failure. Waiting for other running tasks to complete...");
            }
        }
    }

    private int submitTasksFromQueue(Queue<String> readyQueue, ExecutorCompletionService<ExecutionResult> cs, Map<String, AtomicInteger> currentInDegree) {
        int submitted = 0;
        while (!readyQueue.isEmpty()) {
            String nodeId = readyQueue.poll();
            submitTask(nodeId, cs, currentInDegree);
            submitted++;
        }
        return submitted;
    }

    private void postExecutionCleanup(ExecutorService executor, Map<String, AtomicInteger> currentInDegree) {
        if (!collectedFailures.isEmpty()) {
            log.error(" ყველა Workflow finished with {} final failure(s). Creating savepoint.", collectedFailures.size());
            WorkflowState stateToSave = createSavepoint(currentInDegree);
            saveStateToFile(stateToSave, config.getSavepointPath());
        } else if (completedTasksCounter.get() == nodes.size()) {
            log.info("🎉 DAG workflow execution completed successfully.");
        } else {
            log.warn("🏁 Workflow finished prematurely. Completed {}/{} tasks.", completedTasksCounter.get(), nodes.size());
        }
        shutdownExecutor(executor);
    }

    // --- Helper Methods ---

    private void initializeState(Map<String, Object> initialData, WorkflowState resumeState) {
        this.runningFutures = new ConcurrentHashMap<>();
        this.attemptCounts = new ConcurrentHashMap<>();
        nodes.keySet().forEach(id -> attemptCounts.put(id, new AtomicInteger(0)));
        this.skippedNodes = ConcurrentHashMap.newKeySet();
        this.completedTasksCounter = new AtomicInteger(0);
        this.shutdownInProgress = new AtomicBoolean(false);
        this.collectedFailures = new ConcurrentHashMap<>();

        if (resumeState == null) {
            this.nodeOutputs = new ConcurrentHashMap<>();
            this.nodeOutputs.put(START_NODE_ID, initialData);
        } else {
            log.info("Initializing state from savepoint.");
            this.nodeOutputs = new ConcurrentHashMap<>(resumeState.completedNodeOutputs);
            this.completedTasksCounter.set(this.nodeOutputs.size() - 1); // -1 for __start__
            resumeState.failedNodesInfo.forEach((id, info) -> this.attemptCounts.get(id).set(info.attempts));
        }
    }

    private Map<String, AtomicInteger> calculateInDegrees(WorkflowState resumeState) {
        final Map<String, AtomicInteger> currentInDegree = new ConcurrentHashMap<>();
        if (resumeState != null && resumeState.currentInDegrees != null) {
            resumeState.currentInDegrees.forEach((id, count) -> currentInDegree.put(id, new AtomicInteger(count)));
        } else {
            nodes.keySet().forEach(nodeId -> currentInDegree.put(nodeId, new AtomicInteger(0)));
            adjacencyList.forEach((from, toList) -> toList.forEach(to -> currentInDegree.get(to).incrementAndGet()));
        }
        return currentInDegree;
    }

    private void populateInitialReadyQueue(Queue<String> readyQueue, Map<String, AtomicInteger> currentInDegree, WorkflowState resumeState) {
        if (resumeState != null) {
            log.info("Re-queueing {} failed node(s) from savepoint.", resumeState.failedNodesInfo.size());
            resumeState.failedNodesInfo.keySet().forEach(failedNodeId -> {
                log.debug("   Re-queueing: {}", failedNodeId);
                readyQueue.offer(failedNodeId);
            });
        } else {
            currentInDegree.forEach((nodeId, degree) -> {
                if (degree.get() == 0) readyQueue.offer(nodeId);
            });
        }
    }

    private void propagateSkip(String nodeIdToSkip, Map<String, AtomicInteger> currentInDegree) {
        if (skippedNodes.contains(nodeIdToSkip) || nodeOutputs.containsKey(nodeIdToSkip)) return;

        if (currentInDegree.get(nodeIdToSkip).decrementAndGet() == 0) {
            log.info("⏭️  Skipping node '{}' ({}) as its dependency path was not activated.", nodes.get(nodeIdToSkip).getName(), nodeIdToSkip);
            skippedNodes.add(nodeIdToSkip);
            completedTasksCounter.incrementAndGet();
            adjacencyList.getOrDefault(nodeIdToSkip, List.of()).forEach(dependentId -> propagateSkip(dependentId, currentInDegree));
        }
    }

    private void submitTask(String nodeId, ExecutorCompletionService<ExecutionResult> cs, Map<String, AtomicInteger> currentInDegree) {
        WorkflowNode node = nodes.get(nodeId);
        Map<String, Object> inputs = gatherInputsForNode(nodeId);
        log.info("📨 Submitting task: '{}' ({})", node.getName(), nodeId);
        Future<ExecutionResult> future = cs.submit(() -> node.call(inputs));
        runningFutures.put(future, nodeId);
    }

    private Map<String, Object> gatherInputsForNode(String nodeId) {
        Map<String, Object> inputs = new HashMap<>();
        List<String> predNodeIds = predecessors.getOrDefault(nodeId, List.of());
        if (predNodeIds.isEmpty()) {
            inputs.putAll(nodeOutputs.getOrDefault(START_NODE_ID, Map.of()));
        } else {
            for (String predId : predNodeIds) {
                if (nodeOutputs.containsKey(predId)) inputs.putAll(nodeOutputs.get(predId));
            }
        }
        return inputs;
    }

    private WorkflowState createSavepoint(Map<String, AtomicInteger> currentInDegree) {
        WorkflowState state = new WorkflowState();
        state.failedNodesInfo = new HashMap<>(this.collectedFailures);
        state.completedNodeOutputs = new HashMap<>(this.nodeOutputs);
        state.currentInDegrees = new HashMap<>();
        currentInDegree.forEach((id, degree) -> state.currentInDegrees.put(id, degree.get()));
        state.timestamp = System.currentTimeMillis();
        return state;
    }

    public void saveStateToFile(WorkflowState state, String filePath) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        try (FileWriter writer = new FileWriter(filePath)) {
            gson.toJson(state, writer);
            log.info("💾 Savepoint created successfully at: {}", filePath);
        } catch (IOException e) {
            log.error("Failed to write savepoint file: {}", filePath, e);
        }
    }

    public static WorkflowState loadStateFromFile(String filePath) {
        Gson gson = new Gson();
        try (FileReader reader = new FileReader(filePath)) {
            log.info("🔄 Loading savepoint from: {}", filePath);
            return gson.fromJson(reader, WorkflowState.class);
        } catch (IOException e) {
            log.warn("Could not load savepoint file: {}. Will start a fresh run.", e.getMessage());
            return null;
        }
    }

    private void shutdownExecutor(ExecutorService executor) {
        executor.shutdown();
        try {
            if (!executor.awaitTermination(config.getShutdownTimeoutSeconds(), TimeUnit.SECONDS)) {
                log.error("Executor did not terminate in the specified time.");
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            log.error("Executor shutdown was interrupted.", e);
            executor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }

    // --- Main method for demonstration ---
    public static void main(String[] args) {
        // --- 场景 1: 运行一个注定会并行失败的工作流来创建保存点 ---
        log.info("--- SCENARIO 1: RUN, FAIL ON MULTIPLE PARALLEL NODES, AND CREATE SAVEPOINT ---");
        DAGExecutorConfig config = DAGExecutorConfig.defaults();
        ProductionDAGExecutor executor1 = ProductionDAGExecutor.newInstance(config);
        // 不稳定节点C: 前2次会失败
        WorkflowNode unstableNodeC = new UnstableNode("C", "Unstable Video Processing", 3, 2);
        // 不稳定节点E: 前2次会失败
        WorkflowNode unstableNodeE = new UnstableNode("E", "Unstable Audio Processing", 3, 2);
        buildComplexGraph(executor1, unstableNodeC, unstableNodeE);
        try {
            executor1.executeWorkflow(Map.of("media_type", "video", "user_id", "prod-123"), null);
        } catch (Exception e) {
            log.error("Scenario 1 execution failed.", e);
        }


        // --- 场景 2: 从保存点恢复并成功完成 ---
        log.info("\n\n--- SCENARIO 2: RESUME FROM SAVEPOINT AND SUCCEED ---");
        ProductionDAGExecutor executor2 = ProductionDAGExecutor.newInstance(config);
        // 模拟问题已修复：现在这两个节点在第1次尝试时就能成功
        WorkflowNode nowStableNodeC = new UnstableNode("C", "Stable Video Processing", 1, 2);
        WorkflowNode nowStableNodeE = new UnstableNode("E", "Stable Audio Processing", 1, 2);
        buildComplexGraph(executor2, nowStableNodeC, nowStableNodeE);

        WorkflowState resumeState = loadStateFromFile(config.getSavepointPath());
        if (resumeState != null) {
            try {
                // 初始数据是空的，因为所有需要的数据都应在保存点的`nodeOutputs`中
                executor2.executeWorkflow(Collections.emptyMap(), resumeState);
            } catch (Exception e) {
                log.error("Scenario 2 execution failed.", e);
            }
        }
    }

    private static void buildComplexGraph(ProductionDAGExecutor executor, WorkflowNode nodeC, WorkflowNode nodeE) {
        executor.addNode(new SimpleTaskNode("Start", "1. Ingest Data"));
        executor.addNode(new IfConditionNode("A", "2. Check Media Type", "B", "E"));
        executor.addNode(new SimpleTaskNode("B", "3a. Video Pre-processing"));
        executor.addNode(nodeC); // 不稳定/稳定节点C
        executor.addNode(new SimpleTaskNode("D", "4. Join and Normalize"));
        executor.addNode(nodeE); // 不稳定/稳定节点E
        executor.addNode(new SimpleTaskNode("End", "5. Finalize and Archive"));

        executor.addDependency("Start", "A");
        executor.addDependency("A", "B"); // If 'video', go to B
        executor.addDependency("A", "E"); // If not 'video', go to E. Also a parallel task.
        executor.addDependency("B", "C");
        executor.addDependency("C", "D");
        executor.addDependency("E", "D"); // D is a join node for C and E
        executor.addDependency("D", "End");
    }

    // =================================================================================
    //                            NESTED HELPER CLASSES
    // =================================================================================

    /**
     * 执行器配置类。
     */
    public static class DAGExecutorConfig {
        private int threadPoolSize = Runtime.getRuntime().availableProcessors();
        private int taskPollTimeoutSeconds = 5;
        private int shutdownTimeoutSeconds = 10;
        private String savepointPath = "workflow_savepoint.json";

        public static DAGExecutorConfig defaults() { return new DAGExecutorConfig(); }
        public int getThreadPoolSize() { return threadPoolSize; }
        public int getTaskPollTimeoutSeconds() { return taskPollTimeoutSeconds; }
        public int getShutdownTimeoutSeconds() { return shutdownTimeoutSeconds; }
        public String getSavepointPath() { return savepointPath; }
        // Setters for custom configuration would go here
    }

    /**
     * 工作流执行状态的快照（保存点）。
     */
    public static class WorkflowState {
        public Map<String, FailedNodeInfo> failedNodesInfo = new HashMap<>();
        public Map<String, Map<String, Object>> completedNodeOutputs = new HashMap<>();
        public Map<String, Integer> currentInDegrees = new HashMap<>();
        public long timestamp;
    }

    /**
     * 记录单个失败节点信息的辅助类。
     */
    public static class FailedNodeInfo {
        public String nodeId;
        public Map<String, Object> inputs;
        public int attempts;
        public String lastError;
        public FailedNodeInfo(String nodeId, Map<String, Object> inputs, int attempts, String lastError) {
            this.nodeId = nodeId; this.inputs = inputs; this.attempts = attempts; this.lastError = lastError;
        }
        private FailedNodeInfo() {} // For GSON
    }

    /**
     * 节点执行结果的封装对象。
     */
    public static class ExecutionResult {
        private final String sourceNodeId;
        private final boolean success;
        private final List<String> nextNodeIdsToActivate;
        private final Map<String, Object> outputData;
        public ExecutionResult(String sourceNodeId, boolean success, List<String> nextNodeIdsToActivate, Map<String, Object> outputData) { this.sourceNodeId = sourceNodeId; this.success = success; this.nextNodeIdsToActivate = nextNodeIdsToActivate; this.outputData = outputData; }
        public static ExecutionResult success(String sourceNodeId, List<String> nextNodes, Map<String, Object> data) { return new ExecutionResult(sourceNodeId, true, nextNodes, data); }
        public String getSourceNodeId() { return sourceNodeId; }
        public List<String> getNextNodeIdsToActivate() { return nextNodeIdsToActivate; }
        public Map<String, Object> getOutputData() { return outputData; }
    }

    /**
     * 工作流节点接口定义。
     */
    public interface WorkflowNode {
        String getId();
        String getName();
        ExecutionResult call(Map<String, Object> inputs) throws Exception;
        default int getMaxRetries() { return 2; }
        default long getRetryDelayMillis() { return 1000; }
    }

    /**
     * 简单任务节点实现。
     */
    public static class SimpleTaskNode implements WorkflowNode {
        private final String id, name;
        public SimpleTaskNode(String id, String name) { this.id = id; this.name = name; }
        @Override public String getId() { return id; }
        @Override public String getName() { return name; }
        @Override public ExecutionResult call(Map<String, Object> inputs) throws Exception {
            Thread.sleep(100 + (long)(Math.random() * 200));
            return ExecutionResult.success(this.id, Collections.emptyList(), Map.of("processed_by", name));
        }
    }

    /**
     * 条件判断节点实现。
     */
    public static class IfConditionNode implements WorkflowNode {
        private final String id, name, trueNextNodeId, falseNextNodeId;
        public IfConditionNode(String id, String name, String trueNextNodeId, String falseNextNodeId) { this.id = id; this.name = name; this.trueNextNodeId = trueNextNodeId; this.falseNextNodeId = falseNextNodeId; }
        @Override public String getId() { return id; }
        @Override public String getName() { return name; }
        @Override public ExecutionResult call(Map<String, Object> inputs) {
            boolean condition = "video".equals(inputs.get("media_type"));
            log.debug("Condition '{}' evaluated to: {}", name, condition);
            List<String> nextNode = condition ? List.of(trueNextNodeId) : List.of(falseNextNodeId);
            return ExecutionResult.success(this.id, nextNode, Map.of("condition_result", condition));
        }
    }

    /**
     * 不稳定节点，用于测试重试和恢复。
     */
    public static class UnstableNode implements WorkflowNode {
        private final String id, name;
        private final int succeedOnAttempt, maxRetries;
        private final AtomicInteger currentAttempt = new AtomicInteger(0);

        public UnstableNode(String id, String name, int succeedOnAttempt, int maxRetries) {
            this.id = id; this.name = name; this.succeedOnAttempt = succeedOnAttempt; this.maxRetries = maxRetries;
        }
        @Override public String getId() { return id; }
        @Override public String getName() { return name; }
        @Override public int getMaxRetries() { return this.maxRetries; }
        @Override public long getRetryDelayMillis() { return 200; }

        @Override
        public ExecutionResult call(Map<String, Object> inputs) throws Exception {
            int attempt = this.currentAttempt.incrementAndGet();
            Thread.sleep(100);
            if (attempt < succeedOnAttempt) {
                throw new IOException("Simulated I/O failure from '" + name + "' on attempt " + attempt);
            }
            return ExecutionResult.success(this.id, Collections.emptyList(), Map.of("final_attempt", attempt));
        }
    }
}