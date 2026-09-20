package io.tapdata.connector.paimon.service;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** 只保存已分配但未证明关闭的资源；失败根直到进程退出始终强可达，没有 TTL/reaper。 */
public final class PaimonStopResources {
    private static final Map<PaimonStopController, Object> RETAINED = new ConcurrentHashMap<>();
    private final Map<Slot, Object> allocated = new IdentityHashMap<>();
    private static final Object CONSTRUCTING = new Object();

    static void retain(PaimonStopController controller, Object root) { RETAINED.putIfAbsent(controller, root); }
    static Object retainedRoot(PaimonStopController controller) { return RETAINED.get(controller); }
    public static int retainedCount() { return RETAINED.size(); }

    public synchronized Slot reserve(String name) {
        Slot slot = new Slot(this, name);
        allocated.put(slot, CONSTRUCTING);
        return slot;
    }
    public synchronized boolean isEmpty() { return allocated.isEmpty(); }
    public synchronized int size() { return allocated.size(); }

    public Scope scope(String name, PaimonStopController controller) {
        Slot slot = reserve(name);
        return slot.bind(new Scope(slot, controller));
    }

    /** 一个构造/读取生命周期的强引用集合；成功关闭后才从 Service ledger 脱离。 */
    public static final class Scope {
        private final Slot registration;
        private final PaimonStopController controller;
        private final PaimonStopResources handles = new PaimonStopResources();
        private volatile boolean completed;
        public boolean hasResources() { return !completed && !handles.isEmpty(); }
        private Scope(Slot registration, PaimonStopController controller) {
            this.registration = registration;
            this.controller = controller;
        }
        public static Scope standalone(String name) {
            return new PaimonStopResources().scope(name,
                    new PaimonStopController(name, 180, 120, 30));
        }
        public PaimonStopController controller() { return controller; }
        public Slot reserve(String name) { return handles.reserve(name); }
        public <T> T create(String name, PaimonStopController.CheckedSupplier<T> factory) throws Exception {
            Slot slot = reserve(name);
            boolean bound = false;
            try {
                T result = controller.call(name, factory);
                slot.bind(result); // 即使超时已发布，迟到资源也必须绑定，不能先检查 frozen。
                bound = true;
                controller.checkAction("publish " + name);
                return result;
            } finally {
                if (!bound) { slot.released(); }
            }
        }
        public <T> T call(String name, PaimonStopController.CheckedSupplier<T> action) throws Exception {
            return controller.call(name, action);
        }
        public void run(String name, PaimonStopController.CheckedRunnable action) throws Exception {
            controller.run(name, action);
        }
        public void check(String name) { controller.checkAction(name); }
        /** 整个 scope 的资源全部取得实际关闭证明后使用，不能在失败 finally 中调用。 */
        public void completed() { completed = true; registration.released(); }
        public void retain(Throwable failure) { controller.retain(failure, this); }
    }

    public static final class Slot {
        private final PaimonStopResources owner;
        private final String name;
        private boolean released;
        private Slot(PaimonStopResources owner, String name) { this.owner = owner; this.name = name; }
        public <T> T bind(T value) {
            synchronized (owner) {
                if (released || value == null || owner.allocated.get(this) != CONSTRUCTING) {
                    throw new IllegalStateException("Invalid resource binding: " + name);
                }
                owner.allocated.put(this, value);
            }
            return value;
        }
        /** 仅在分配未发生，或资源实际关闭之后调用；不能放在无条件 finally 中。 */
        public void released() {
            synchronized (owner) {
                released = true;
                owner.allocated.remove(this);
            }
        }
    }
}
