package im.unx.perf.util;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;


public class CustomThreadFactory implements ThreadFactory {

    private static final AtomicInteger poolNumber = new AtomicInteger(1);
    private final ThreadGroup group;
    private final AtomicInteger threadNumber = new AtomicInteger(1);
    private final String namePrefix;

    private CustomThreadFactory(ThreadGroup group, String prefix) {
        if (group == null) {
            SecurityManager s = System.getSecurityManager();
            this.group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
        } else {
            this.group = group;
        }
        if (prefix == null) {
            this.namePrefix = "custom-" + poolNumber.getAndIncrement() + "-thread-";
        } else {
            this.namePrefix = prefix + "-pool_" + poolNumber.getAndIncrement() + "-thread-";
        }
    }

    public CustomThreadFactory(String prefix) {
        this(null, prefix);
    }

    @Override
    public Thread newThread(Runnable r) {

        // 线程组中活跃的线程数
//      this.group.activeCount();

        Thread t = new Thread(this.group, r, this.namePrefix + this.threadNumber.getAndIncrement(), 0);
        t.setDaemon(true);
        if (t.getPriority() != Thread.NORM_PRIORITY) {
            t.setPriority(Thread.NORM_PRIORITY);
        }

        return t;
    }
}
