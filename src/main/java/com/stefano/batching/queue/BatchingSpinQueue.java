package com.stefano.batching.queue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Function;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Author stefanofranz
 */
public class BatchingSpinQueue<I, O> {

    private final BlockingQueue<SubmittedTaskPairing<I, O>> backingQueue;

    private final ExecutorService drainingService;
    private final AtomicReference<Runnable> drainingRunnableHolder = new AtomicReference<>();
    private final ReentrantLock notEmptyLock = new ReentrantLock(true);
    private final Condition notEmpty = notEmptyLock.newCondition();
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchingSpinQueue.class);
    private final long futureTimeout;

    public BatchingSpinQueue(int size,
                             int batchSize,
                             int milliSpinTime,
                             ExecutorService drainingService,
                             Consumer<Collection<SubmittedTaskPairing<I, O>>> batchConsumer, long timeoutSize, TimeUnit unit) {

        this.drainingService = drainingService;
        backingQueue = new ArrayBlockingQueue<>(size);
        Runnable drainingRunnable = () -> {
            try {
                //instantly schedule another run of this, this will ensure that during periods of heavy load
                //we have a runnable for each consumer thread waiting
                //the fact that we use await and signal, rather than signalAll means it will only one thread gets woken up at most per insert
                drainingService.submit(this.drainingRunnableHolder.get());
                if (backingQueue.isEmpty()) {
                    //only wait for items if queue is empty
                    long startTime = System.currentTimeMillis();
                    try {
                        notEmptyLock.lockInterruptibly();
                        notEmpty.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        notEmptyLock.unlock();
                    }
                    long timeSlept = System.currentTimeMillis() - startTime;
                    try {
                        Thread.sleep(Math.max(0, milliSpinTime - timeSlept));
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }

                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("slept for: " + (System.currentTimeMillis() - startTime) + "ms waiting for items");
                    }
                }
                List<SubmittedTaskPairing<I, O>> theBatch = new ArrayList<>(batchSize);
                int drainedItems = backingQueue.drainTo(theBatch, batchSize);
                try {
                    if (LOGGER.isTraceEnabled()) {
                        LOGGER.trace("Submitting " + drainedItems + " to batch handler ");
                    }
                    if (drainedItems > 0) {
                        batchConsumer.accept(theBatch);
                    }
                } catch (Exception e) {
                    LOGGER.error("exception during batch processing, batch lost", e);
                    theBatch.stream().forEach(pair -> {
                        if (!pair.getFuture().isDone()) {
                            pair.getFuture().completeExceptionally(e);
                        }
                    });
                }
            } catch (Exception e) {
                LOGGER.error("Failure during drain", e);
            }
        };
        futureTimeout = unit.toMillis(timeoutSize);
        drainingRunnableHolder.set(drainingRunnable);
        this.drainingService.submit(this.drainingRunnableHolder.get());
    }

    public BatchingSpinQueue(int size,
                             int batchSize,
                             int milliSpinTime,
                             int consumerThreads,
                             Consumer<Collection<SubmittedTaskPairing<I, O>>> batchConsumer,
                             long timeoutSize, TimeUnit unit) {
        this(size, batchSize, milliSpinTime, Executors.newFixedThreadPool(consumerThreads), batchConsumer, timeoutSize, unit);
    }

    public CompletableFuture<O> push(I item) {
        try {
            CompletableFuture<O> completableFuture = new CompletableFuture<>();
            SubmittedTaskPairing<I, O> pairing = new SubmittedTaskPairing<>(item, completableFuture);

            backingQueue.put(pairing);
            notEmptyLock.lockInterruptibly();
            notEmpty.signal();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("Added item to queue, current size: " + this.backingQueue.size());
            }
            return within(completableFuture, futureTimeout);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            notEmptyLock.unlock();
        }
    }

    public static class SubmittedTaskPairing<I, O> {
        private final I input;
        private final CompletableFuture<O> future;

        private SubmittedTaskPairing(I input, CompletableFuture<O> future) {
            this.input = input;
            this.future = future;
        }

        public CompletableFuture<O> getFuture() {
            return future;
        }

        public I getQueuedItem() {
            return input;
        }
    }

    public static <T> CompletableFuture<T> within(CompletableFuture<T> future, long timeout) {
        final CompletableFuture<T> timeoutFuture = failAfter(timeout);
        return future.applyToEither(timeoutFuture, Function.identity());
    }

    public static <T> CompletableFuture<T> failAfter(long timeout) {
        final CompletableFuture<T> promise = new CompletableFuture<>();
        scheduler.schedule(() -> {
            final TimeoutException ex = new TimeoutException("Timeout after " + timeout + " millis");
            return promise.completeExceptionally(ex);
        }, timeout, TimeUnit.MILLISECONDS);
        return promise;
    }

    // *REALLY* Should be enough to have a single thread managing all timeouts in system.
    // technically we should use the system wide fork join
    private static final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1);
}
