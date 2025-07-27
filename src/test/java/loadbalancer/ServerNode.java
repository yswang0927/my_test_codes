package loadbalancer;

public class ServerNode<T> {
    final T endpoint;
    final int weight;
    final int maxFails;
    final long failTimeoutSeconds;
    final long failTimeoutMillis;
    final boolean isBackup;
    final boolean isDown;
    int maxConns = 0;

    // State
    int currentWeight;
    int effectiveWeight;
    int fails;
    boolean isDownState;
    long checkedTimestamp;
    int conns;

    public ServerNode(T endpoint, int weight, int maxFails, long failTimeoutSeconds) {
        this(endpoint, weight, 0, maxFails, failTimeoutSeconds, false, false);
    }

    public ServerNode(T endpoint, int weight, int maxConns, int maxFails, long failTimeoutSeconds, boolean isBackup, boolean isDown) {
        this.endpoint = endpoint;
        this.weight = weight; // 配置的静态权重
        this.maxConns = maxConns; // 配置的静态最大连接数
        this.maxFails = maxFails; // 配置的静态允许失败次数
        this.failTimeoutSeconds = failTimeoutSeconds;
        this.failTimeoutMillis = failTimeoutSeconds * 1000; // 配置的静态失败超时时间范围
        this.isBackup = isBackup;
        this.isDown = isDown;

        // Initial state
        this.currentWeight = 0; // 当前权重
        this.effectiveWeight = this.weight; // 有效权重
        this.fails = 0; // 当前失败次数
        this.isDownState = false; // 当前状态是否被标记为不可用了
        this.checkedTimestamp = 0; // 上次检查时间
        this.conns = 0;  // 当前连接数量
    }

    public T getEndpoint() {
        return endpoint;
    }

    public int getWeight() {
        return weight;
    }

    public int getMaxFails() {
        return maxFails;
    }

    public long getFailTimeoutSeconds() {
        return failTimeoutSeconds;
    }

    public int getEffectiveWeight() {
        return effectiveWeight;
    }

    public void setEffectiveWeight(int effectiveWeight) {
        this.effectiveWeight = effectiveWeight;
    }

    public int getFails() {
        return fails;
    }

    public void setFails(int fails) {
        this.fails = fails;
    }

    public boolean isDown() {
        return isDown;
    }

    public void setDownState(boolean down) {
        isDownState = down;
    }

    public long getCheckedTimestamp() {
        return checkedTimestamp;
    }

    public void setCheckedTimestamp(long checkedTimestamp) {
        this.checkedTimestamp = checkedTimestamp;
    }

    public boolean isBackup() {
        return isBackup;
    }

    public int getMaxConns() {
        return maxConns;
    }

    public void setMaxConns(int maxConns) {
        this.maxConns = maxConns;
    }

    @Override
    public String toString() {
        return String.format("Server[endpoint=%s, w=%d, ew=%d, fails=%d, down=%b]",
                endpoint, weight, effectiveWeight, fails, isDown);
    }

}
