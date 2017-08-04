/**
 *
 * GcPerf.java
 * @date 3/31/15 19:26
 * @author leo [liuy@xiaomi.com]
 * [CopyRight] All Rights Reserved.
 */

package com.xiaomi.msg.sys;

import com.xiaomi.common.perfcounter.PerfCounter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Lazy Gc perf used to get perf-counter of system garbage collection, by which lazy means only query when user calls #dump(Map).
 * Usage: in your Thrift service, override the #getPerfCounters(), and add following code:
 * <code>
 * GcPerf.dump(counter);
 * </code>
 * Reference: git/xmpush/base.
 *
 * @author leo
 */
public class GcPerf {
    protected static final Logger logger = LoggerFactory.getLogger(GcPerf.class);

    // gc情况perf-counter name
    protected static final String YGCC = "sys.gc.young.count.COUNTER";
    protected static final String YGCT = "sys.gc.young.time.COUNTER";
    protected static final String FGCC = "sys.gc.full.count.COUNTER";
    protected static final String FGCT = "sys.gc.full.time.COUNTER";
    protected static final ScheduledExecutorService backendService = Executors.newSingleThreadScheduledExecutor();

    /**
     * Type definition (Only contains oracle sun's jvm).
     */
    private enum GcType {
        // young gc
        Serial("Copy"), ParNew("ParNew"), Parallel("PS Scavenge"),

        // full gc
        MSC("MarkSweepCompact"), PSMS("PS MarkSweep"), CMS("ConcurrentMarkSweep");

        private static final String FullClassNamePrefix = "java.lang:type=GarbageCollector,name=";

        ObjectName name;

        private GcType(String name) {
            try {
                this.name = new ObjectName(FullClassNamePrefix + name);
            } catch (MalformedObjectNameException ex) {
                throw new IllegalArgumentException(ex);
            }
        }
    }


    /**
     * Get full-gc and young-gc counts and time.
     *
     * @param counters container to store.
     * @return
     */
    public static Map<String, Long> dump(Map<String, Long> counters) {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        long ygcc = 0, ygct = 0, fgcc = 0, fgct = 0;
        for (GcType type : GcType.values()) {
            try {
                Long c = (Long) mbs.getAttribute(type.name, "CollectionCount");
                Long t = (Long) mbs.getAttribute(type.name, "CollectionTime");
                switch (type) {
                    case Serial: case ParNew: case Parallel: {
                        ygcc += c;
                        ygct += t;
                        break;
                    }
                    case MSC: case PSMS: case CMS: {
                        fgcc += c;
                        fgct += t;
                        break;
                    }
                    default:
                        break;
                }
            } catch (Exception ex) { // MBeanException AttributeNotFoundException InstanceNotFoundException ReflectionException
                logger.debug("Query gc counters got exception", ex);
            }
        }
        counters.put(YGCC, ygcc);
        counters.put(YGCT, ygct);
        counters.put(FGCC, fgcc);
        counters.put(FGCT, fgct);
        return counters;
    }

    /**
     * Start reporting  gc perf-counters to falcon
     * @param interval  reporting interval of seconds
     */
    public static void startReporter(int interval) {
        class ReportTask implements Runnable {
            @Override
            public void run() {
                Map<String, Long> counters = new HashMap<String, Long>();
                counters = dump(counters);

                for(Map.Entry<String, Long> entry : counters.entrySet()) {
                    PerfCounter.count(entry.getKey(), entry.getValue());
                }
            }
        }

        backendService.scheduleWithFixedDelay(new ReportTask(), 0, interval, TimeUnit.SECONDS);
    }

    public static void stopReporter() {
        backendService.shutdown();
    }
}
