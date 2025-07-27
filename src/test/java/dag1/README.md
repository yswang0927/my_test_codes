工作流自动化对于协调复杂的业务流程至关重要，从电子邮件活动到 ETL 管道。有向无环图（DAG）执行引擎通过管理依赖关系、并行化独立任务和实现容错性来确保高效的任务执行。在本文中，我们将使用 Java 并发（ExecutorService）在 Java 中构建一个基于 DAG 的工作流执行引擎。

https://medium.com/@amit.anjani89/building-a-dag-based-workflow-execution-engine-in-java-with-spring-boot-ba4a5376713d

为何使用 DAG 进行工作流执行？
- 节点代表任务。
- 边定义依赖关系。
- 不存在环，确保有序执行序列。

主要优势：
- 依赖管理——确保任务按正确顺序执行。
- 并行执行 — 独立任务同时运行，提高效率。
- 容错性 — 支持重试和日志记录，以便从故障中恢复。
- 可扩展性 — 在 Spark 和 Kubernetes 等分布式环境中表现良好。

在我们的实现中，工作流以结构化的 JSON 格式存储在后端数据库中。以下是一个示例表示：

```json
{
  "workflowId": "123",
  "name": "Sample Workflow",
  "nodes": [
    { "id": "1", "type": "start", "data": { "label": "Start" } },
    { "id": "2", "type": "action", "data": { "label": "Send Email" } },
    { "id": "3", "type": "condition", "data": { "label": "Check Response" } }
  ],
  "edges": [
    { "source": "1", "target": "2" },
    { "source": "2", "target": "3" }
  ]
}
```

工作流引擎架构:
- WorkflowNode — 表示具有依赖关系的任务。
- DAGExecutor — 管理任务执行并强制执行依赖关系。
- ExecutorService — 异步运行任务。
- 拓扑排序 — 确定执行顺序。

