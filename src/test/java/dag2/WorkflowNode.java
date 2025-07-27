package dag2;

import java.util.Map;

// 升级为接口，输入为上游节点数据的集合
public interface WorkflowNode {
    String getId();
    String getName();
    ExecutionResult call(Map<String, Object> inputs) throws Exception;

    // 新增：获取最大重试次数
    default int getMaxRetries() {
        return 2; // 默认2次
    }

    // 新增：获取重试延迟（毫秒）
    default long getRetryDelayMillis() {
        return 1000; // 默认1秒
    }

}

