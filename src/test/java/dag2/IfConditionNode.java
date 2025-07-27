package dag2;

import java.util.List;
import java.util.Map;

/**
 * 条件判断节点
 */
public class IfConditionNode implements WorkflowNode {
    private final String id;
    private final String name;
    private final String trueNextNodeId;
    private final String falseNextNodeId;

    public IfConditionNode(String id, String name, String trueNextNodeId, String falseNextNodeId) {
        this.id = id;
        this.name = name;
        this.trueNextNodeId = trueNextNodeId;
        this.falseNextNodeId = falseNextNodeId;
    }

    @Override
    public ExecutionResult call(Map<String, Object> inputs) {
        System.out.println("▶️ Executing Condition Node: " + name + " with inputs: " + inputs);
        // 假设条件是检查输入中'value'是否大于10
        int value = (int) inputs.getOrDefault("value", 0);

        List<String> nextNode;
        String result;
        if (value > 10) {
            System.out.println("...Condition is TRUE (" + value + " > 10)");
            nextNode = List.of(trueNextNodeId);
            result = "TRUE";
        } else {
            System.out.println("...Condition is FALSE (" + value + " <= 10)");
            nextNode = List.of(falseNextNodeId);
            result = "FALSE";
        }

        // 将判断结果也作为输出传递下去
        Map<String, Object> output = Map.of("condition_result", result);
        return ExecutionResult.success(this.id, nextNode, output);
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
