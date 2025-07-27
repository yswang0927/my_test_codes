package dag1.gemini;

import java.util.concurrent.Callable;

/**
 * 第1步: 优化 WorkflowNode (可选，但推荐)
 * 我们简化节点定义，让它只关注自身的业务逻辑。依赖关系完全由DAGExecutor管理。
 */
public class WorkflowNode implements Callable<Boolean> {
    private final String id;
    private final String name;
    // 模拟一个可能失败的操作
    private final boolean shouldFail;

    public WorkflowNode(String id, String name) {
        this(id, name, false);
    }

    public WorkflowNode(String id, String name, boolean shouldFail) {
        this.id = id;
        this.name = name;
        this.shouldFail = shouldFail;
    }

    public String getId() {
        return id;
    }

    @Override
    public Boolean call() throws Exception {
        System.out.println("▶️ Executing Node: " + name + " (Thread: " + Thread.currentThread().getName() + ")");
        Thread.sleep(1000 + (int)(Math.random() * 1000)); // 模拟不同的执行时间

        if (shouldFail) {
            System.out.println("❌ Node " + name + " is configured to fail.");
            throw new RuntimeException("Simulated failure in " + name);
        }

        System.out.println("✅ Node " + name + " completed successfully.");
        return true;
    }
}
