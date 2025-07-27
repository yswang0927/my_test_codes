package dag1;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * 定义工作流节点.
 * 每个节点代表一个工作流任务，封装其 ID、名称、依赖关系和执行逻辑。
 */
public class WorkflowNode implements Callable<Boolean> {
    private final String id;
    private final String name;
    private final List<String> dependencies;

    public WorkflowNode(String id, String name, List<String> dependencies) {
        this.id = id;
        this.name = name;
        this.dependencies = dependencies;
    }

    public String getId() {
        return id;
    }

    public List<String> getDependencies() {
        return dependencies;
    }

    @Override
    public Boolean call() throws Exception {
        System.out.println("Executing Node: " + name);
        Thread.sleep(1000); // Simulate processing
        return true;
    }
}
