//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.paimon.utils;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.util.Arrays;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;

public class ThreadUtils {
    public static String currentStackString() {
        StackTraceElement[] trace = (StackTraceElement[])Thread.getAllStackTraces().get(Thread.currentThread());
        StringBuilder builder = new StringBuilder();

        for(StackTraceElement traceElement : trace) {
            builder.append("\nat ").append(traceElement);
        }

        return builder.toString();
    }

    public static void errorLogThreadDump(Logger logger) {
        ThreadInfo[] perThreadInfo = ManagementFactory.getThreadMXBean().dumpAllThreads(true, true);
        logger.error("Thread dump: \n{}", Arrays.stream(perThreadInfo).map(Object::toString).collect(Collectors.joining()));
    }

    public static boolean stackContains(String name) {
        StackTraceElement[] ss = (new RuntimeException()).getStackTrace();

        for(StackTraceElement s : ss) {
            if (s.toString().contains(name)) {
                return true;
            }
        }

        return false;
    }

    public static ThreadFactory newDaemonThreadFactory(String prefix) {
        final ThreadFactory namedFactory = getNamedThreadFactory(prefix);
        return new ThreadFactory() {
            public Thread newThread(Runnable r) {
                Thread t = namedFactory.newThread(r);
                if (!t.isDaemon()) {
                    t.setDaemon(true);
                }

                if (t.getPriority() != 5) {
                    t.setPriority(5);
                }

                return t;
            }
        };
    }

    private static ThreadFactory getNamedThreadFactory(final String prefix) {
        return new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            public Thread newThread(Runnable r) {
                String name = prefix + "-t" + this.threadNumber.getAndIncrement();
                return new Thread(null, r, name);
            }
        };
    }
}
