package cms.samples;

package be.speos.mmbe.mmbeloader.parser;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

@Component
@Scope("prototype")
public class MmbeThreadPoolExecutor extends ThreadPoolExecutor {
    private Logger logger = LoggerFactory.getLogger(MmbeThreadPoolExecutor.class);
    /**
     * Core pool size of the executor. Initialize with the available processors
     */
    private static int CORE_POOL_SIZE = Runtime.getRuntime().availableProcessors()*1;

    /**
     * Maximum pool size of the executor. Initialize with the available
     * processors
     */
    private static int MAXIMUM_POOL_SIZE = Runtime.getRuntime().availableProcessors()*1;

    /**
     * Time the threads of the executor can be idle
     */
    private static long KEEP_ALIVE_TIME = 10;
    private final List<Throwable> uncaughtExceptions =
            Collections.synchronizedList(new LinkedList<>());
    private AtomicLong completedCount = new AtomicLong(0);
    private final Semaphore semaphore;

    /**
     * Constructor of the class
     */
    public MmbeThreadPoolExecutor() {

        super(CORE_POOL_SIZE, MAXIMUM_POOL_SIZE, KEEP_ALIVE_TIME,
                TimeUnit.SECONDS, new ArrayBlockingQueue<>(50), new RejectedExecutionHandler() {
                    @Override
                    public void rejectedExecution(Runnable runnable, ThreadPoolExecutor threadPoolExecutor) {
                        try {
                            Thread.sleep(1000);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                        threadPoolExecutor.submit(runnable);
                    }
                });

        semaphore = new Semaphore(CORE_POOL_SIZE + 50);
    }

    @Override
    public void execute(final Runnable task) {
        boolean acquired = false;
        do {
            try {
                semaphore.acquire();
                acquired = true;
            } catch (final InterruptedException e) {

            }
        } while (!acquired);
        try {
            super.execute(task);
        } catch (final RejectedExecutionException e) {

            semaphore.release();
            throw e;
        }
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        super.afterExecute(r, t);
        if (t == null && r instanceof Future<?>) {
            try {
                Future<?> future = (Future<?>) r;
                if (future.isDone()) {
                    future.get();
                }
            } catch (CancellationException ce) {
                t = ce;

            } catch (ExecutionException ee) {
                t = ee.getCause();

            } catch (InterruptedException ie) {

                Thread.currentThread().interrupt();
            }
        }

        if (t != null) {
            uncaughtExceptions.add(t);
            logger.error("The exception " + t.getMessage() + " has been thrown.");
        } else {
            long count = completedCount.incrementAndGet();
            logger.info(count + "Batches completed");
            //System.out.println(count + "Batches completed");
        }
        semaphore.release();

    }

    public List<Throwable> getUncaughtExceptions() {
        return Collections.unmodifiableList(uncaughtExceptions);
    }
}
