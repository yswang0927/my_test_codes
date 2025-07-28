package loadbalancer;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class ServerNode<T> {
    final T endpoint;
    final int weight;
    final int maxFails;
    final long failTimeoutMillis;
    final boolean isBackup;
    final boolean isDown;
    final int maxConns;

    // State
    volatile int currentWeight;
    volatile int effectiveWeight;
    volatile int fails;
    volatile boolean isDownState;
    volatile long checkedTimestamp;
    final AtomicInteger conns = new AtomicInteger(0);

    // 每个ServerNode对象一把锁,用于加锁修改对象状态属性
    final Lock lock = new ReentrantLock();

    public ServerNode(T endpoint, int weight, int maxFails, long failTimeoutSeconds) {
        this(endpoint, weight, maxFails, failTimeoutSeconds, 0, false, false);
    }

    public ServerNode(T endpoint, int weight, int maxFails, long failTimeoutSeconds, int maxConns, boolean isBackup, boolean isDown) {
        this.endpoint = endpoint;
        this.weight = weight;     // 配置的静态权重
        this.maxFails = maxFails; // 配置的静态允许失败次数
        this.failTimeoutMillis = failTimeoutSeconds * 1000; // 配置的静态失败超时时间范围
        this.maxConns = maxConns; // 配置的静态最大连接数
        this.isBackup = isBackup; // 是否是备用节点
        this.isDown = isDown;     // 节点是否下线了

        // Initial state
        this.currentWeight = 0;             // 当前权重
        this.effectiveWeight = this.weight; // 有效权重
        this.fails = 0;                     // 当前失败次数
        this.isDownState = isDown;          // 当前状态是否被标记为不可用了
        this.checkedTimestamp = 0;          // 上次检查时间
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ServerNode<?> that)) {
            return false;
        }
        return Objects.equals(endpoint, that.endpoint);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(endpoint);
    }

    @Override
    public String toString() {
        return String.format("Server[endpoint=%s, w=%d, ew=%d, fails=%d, down=%b]",
                endpoint, weight, effectiveWeight, fails, isDown);
    }

}
