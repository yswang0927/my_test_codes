package dag2;

// ExecutionResult.java

import java.util.List;
import java.util.Map;

public class ExecutionResult {
    private final String sourceNodeId; // 新增：明确的源节点ID
    private final boolean success;
    private final List<String> nextNodeIdsToActivate;
    private final Map<String, Object> outputData;

    public ExecutionResult(String sourceNodeId, boolean success, List<String> nextNodeIdsToActivate, Map<String, Object> outputData) {
        this.sourceNodeId = sourceNodeId; // 初始化
        this.success = success;
        this.nextNodeIdsToActivate = nextNodeIdsToActivate;
        this.outputData = outputData;
    }

    // 静态工厂方法更新
    public static ExecutionResult success(String sourceNodeId, List<String> nextNodes, Map<String, Object> data) {
        return new ExecutionResult(sourceNodeId, true, nextNodes, data);
    }

    public static ExecutionResult failure(String sourceNodeId) {
        return new ExecutionResult(sourceNodeId, false, List.of(), Map.of());
    }

    // Getters...
    public String getSourceNodeId() {
        return sourceNodeId;
    }

    public boolean isSuccess() {
        return success;
    }

    public List<String> getNextNodeIdsToActivate() {
        return nextNodeIdsToActivate;
    }

    public Map<String, Object> getOutputData() {
        return outputData;
    }
}