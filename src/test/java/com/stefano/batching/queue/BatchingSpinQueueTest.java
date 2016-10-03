package com.stefano.batching.queue;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

/**
 * Author stefanofranz
 */
public class BatchingSpinQueueTest {

    @Test(expected = CompletionException.class)
    public void shouldReturnAFutureWhichTimesOutAfterTimeout() throws Throwable {
        BatchingSpinQueue<Integer, String> queue = new BatchingSpinQueue<>(100, 1, 100, Executors.newSingleThreadExecutor(), batch -> {
            //do nothing
        }, 100, TimeUnit.MILLISECONDS);
        CompletableFuture<String> push = queue.push(1);
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Throwable> thrown = new AtomicReference<>();
        push.exceptionally(t -> {
            thrown.set(t);
            latch.countDown();
            return null;
        });
        latch.await();
        Assert.assertThat(thrown.get().getCause(), is(instanceOf(TimeoutException.class)));
        throw thrown.get();
    }

    @Test
    public void shouldBatchIntoSpecifiedSizes() throws Exception {
        CountDownLatch latch = new CountDownLatch(100);
        BatchingSpinQueue<Integer, String> queue = new BatchingSpinQueue<>(1000, 10, 100, Executors.newSingleThreadExecutor(), batch -> {
            for (BatchingSpinQueue.SubmittedTaskPairing<Integer, String> pairing : batch) {
                latch.countDown();
            }
            Assert.assertThat(batch.size(), is(10));
            //do nothing
        }, 100, TimeUnit.MILLISECONDS);
        IntStream.range(0, 100).forEach(queue::push);
        latch.await();
    }

    @Test
    public void shouldUseNumberOfThreadsSpecifiedToConsumeBatches() throws Exception {
        Map<Thread, Boolean> threadMap = new ConcurrentHashMap<>();
        int numberOfItems = 10000;
        CountDownLatch latch = new CountDownLatch(numberOfItems);
        int consumerThreads = 8;
        BatchingSpinQueue<Integer, String> queue = new BatchingSpinQueue<>(20000, 100, 100, consumerThreads, batch -> {
            for (BatchingSpinQueue.SubmittedTaskPairing<Integer, String> pairing : batch) {
                latch.countDown();
            }
            System.out.println("Consuming " + batch.size() + " items in Thread: " + Thread.currentThread().getName());
            threadMap.put(Thread.currentThread(), true);
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
            }
        }, 100, TimeUnit.MILLISECONDS);
        IntStream.range(0, numberOfItems).forEach(queue::push);
        latch.await();
        Assert.assertThat(threadMap.size(), is(consumerThreads));
    }

    @Test
    public void shouldReturnFutureWhichIsCompletedAfterTaskIsConsumed() throws Exception {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        BatchingSpinQueue<Integer, String> queue = new BatchingSpinQueue<>(100, 10, 10, 8, batch -> {
            for (BatchingSpinQueue.SubmittedTaskPairing<Integer, String> pairing : batch) {
                CompletableFuture<String> future = pairing.getFuture();
                System.out.println("Consuming in thread: " + Thread.currentThread().getName());
                future.complete(pairing.getQueuedItem().toString());
            }
        }, 100, TimeUnit.MILLISECONDS);
        CompletableFuture<String> task = queue.push(99);
        task.thenAccept(string -> {
            System.out.println("Performing checks in thread: " + Thread.currentThread().getName());
            Assert.assertThat(string, is(equalTo("99")));
            countDownLatch.countDown();
        });
        countDownLatch.await();
    }
}
