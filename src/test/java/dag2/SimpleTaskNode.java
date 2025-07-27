package dag2;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 普通任务节点
 */
public class SimpleTaskNode implements WorkflowNode {
    private final String id;
    private final String name;

    public SimpleTaskNode(String id, String name) {
        this.id = id;
        this.name = name;
    }

    @Override
    public ExecutionResult call(Map<String, Object> inputs) throws Exception {
        System.out.println("▶️ Executing Simple Node: " + name + " with inputs: " + inputs);
        Thread.sleep(500);
        Map<String, Object> output = Map.of("status", "OK", "processed_by", name);
        // 包含自己的ID
        return ExecutionResult.success(this.id, Collections.emptyList(), output);
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }
}
