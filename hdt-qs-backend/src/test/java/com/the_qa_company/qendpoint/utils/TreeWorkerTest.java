package com.the_qa_company.qendpoint.utils;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;

@RunWith(Parameterized.class)
public class TreeWorkerTest {
    @Parameterized.Parameters(name = "test {0} worker(s)")
    public static Collection<Object> params() {
        return Arrays.asList(1, 8);
    }

    private static class SyncSupplierTest implements Supplier<Integer> {
        private final int max;
        private final long sleep;
        private int val;
        private boolean inUse = false;

        public SyncSupplierTest(int max, long sleep) {
            this.max = max;
            this.sleep = sleep;
        }

        @Override
        public Integer get() {
            synchronized (this) {
                Assert.assertFalse(inUse);
                inUse = true;
            }
            sleepOrThrow(sleep);
            synchronized (this) {
                Assert.assertTrue(inUse);
                inUse = false;
            }
            if (val == max) {
                return null;
            }
            return ++val;
        }
    }

    private static class CountCatTest implements TreeWorker.TreeWorkerCat<Integer> {
        int call = 0;

        @Override
        public Integer construct(Integer a, Integer b) {
            synchronized (this) {
                call++;
            }
            return a + b;
        }
    }

    private static class CountComparator implements Comparator<Integer> {
        int call = 0;

        @Override
        public int compare(Integer o1, Integer o2) {
            synchronized (this) {
                call++;
            }
            return Integer.compare(o1, o2);
        }
    }

    private final int workers;

    public TreeWorkerTest(int workers) {
        this.workers = workers;
    }

    @Test
    public void syncSupplierTest() throws InterruptedException, TreeWorker.TreeWorkerException {
        TreeWorker.TreeWorkerCat<Integer> cat = Integer::sum;
        int max = 10;
        Supplier<Integer> supplier = new SyncSupplierTest(max, 20L);

        TreeWorker<Integer> worker = new TreeWorker<>(cat, supplier, null, workers);
        worker.start();
        Integer result = worker.waitToComplete();
        Assert.assertTrue(worker.isCompleted());
        Assert.assertNotNull(result);
        Assert.assertEquals(max * (max + 1) / 2, result.intValue());
    }

    @Test(expected = TreeWorker.TreeWorkerException.class)
    public void noElementSupplierTest() throws TreeWorker.TreeWorkerException {
        TreeWorker.TreeWorkerCat<Integer> cat = Integer::sum;
        int max = 0;
        Supplier<Integer> supplier = new SyncSupplierTest(max, 20L);

        // should crash because the supplier won't return any value to merge
        new TreeWorker<>(cat, supplier, null, workers);
    }

    @Test
    public void oneElementSupplierTest() throws InterruptedException, TreeWorker.TreeWorkerException {
        TreeWorker.TreeWorkerCat<Integer> cat = Integer::sum;
        int max = 1;
        Supplier<Integer> supplier = new SyncSupplierTest(max, 20L);

        TreeWorker<Integer> worker = new TreeWorker<>(cat, supplier, null, workers);
        worker.start();
        Integer result = worker.waitToComplete();
        Assert.assertTrue(worker.isCompleted());
        Assert.assertNotNull(result);
        Assert.assertEquals(1, result.intValue());
    }

    @Test
    public void catExceptionTest() throws InterruptedException, TreeWorker.TreeWorkerException {
        final String error = "azertyuiolpmzzz";
        TreeWorker.TreeWorkerCat<Integer> cat = (a, b) -> {
            throw new RuntimeException(error);
        };
        int max = 1;
        Supplier<Integer> supplier = new SyncSupplierTest(max, 20L);

        TreeWorker<Integer> worker = new TreeWorker<>(cat, supplier, null, workers);
        worker.start();
        try {
            worker.waitToComplete();
        } catch (TreeWorker.TreeWorkerException e) {
            Assert.assertEquals(error, e.getCause().getMessage());
        }
        Assert.assertTrue(worker.isCompleted());
    }

    @Test
    public void countTest() throws InterruptedException, TreeWorker.TreeWorkerException {
        CountCatTest cat = new CountCatTest();
        int max = 1 << 5;
        Supplier<Integer> supplier = new SyncSupplierTest(max, 2L);

        TreeWorker<Integer> worker = new TreeWorker<>(cat, supplier, null, workers);
        worker.start();
        Integer result = worker.waitToComplete();
        Assert.assertTrue(worker.isCompleted());
        Assert.assertNotNull(result);
        Assert.assertEquals(max * (max + 1) / 2, result.intValue());
        Assert.assertEquals(max - 1, cat.call);
    }

    @Test
    public void countAscendTest() throws InterruptedException, TreeWorker.TreeWorkerException {
        CountCatTest cat = new CountCatTest();
        int max = 1 << 5 - 1;
        Supplier<Integer> supplier = new SyncSupplierTest(max, 2L);

        TreeWorker<Integer> worker = new TreeWorker<>(cat, supplier, null, workers);
        worker.start();
        Integer result = worker.waitToComplete();
        Assert.assertTrue(worker.isCompleted());
        Assert.assertNotNull(result);
        Assert.assertEquals(max * (max + 1) / 2, result.intValue());
        Assert.assertEquals(max - 1, cat.call);
    }

    @Test
    public void deleteTest() throws TreeWorker.TreeWorkerException, InterruptedException {
        int max = 10;
        Set<Integer> elements = new HashSet<>();
        TreeWorker.TreeWorkerCat<Integer> cat = (a, b) -> {
            synchronized (elements) {
                elements.remove(a * max);
                elements.remove(b * max);
                int next = (a + b);
                elements.add(next * max);
                return next;
            }
        };
        Supplier<Integer> supplier = new Supplier<>() {
            int value = 0;

            @Override
            public Integer get() {
                if (value == max) {
                    return null;
                }
                int v = ++value;
                synchronized (elements) {
                    elements.add(v * max);
                }
                return v;
            }
        };

        Consumer<Integer> delete = elements::remove;

        TreeWorker<Integer> worker = new TreeWorker<>(cat, supplier, delete, workers);
        worker.start();
        Integer result = worker.waitToComplete();
        Assert.assertTrue(worker.isCompleted());
        Assert.assertNotNull(result);
        Assert.assertEquals(1, elements.size());
        Assert.assertEquals(result * max, elements.iterator().next().intValue());
        Assert.assertEquals(max * (max + 1) / 2, result.intValue());
    }

    @Test
    public void mergeSortTest() throws TreeWorker.TreeWorkerException, InterruptedException {
        Random rnd = new Random(42);
        int count = 20;
        List<Integer> values = new ArrayList<>();
        List<Integer> lst = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            int v = rnd.nextInt();
            values.add(v);
            lst.add(v);
        }
        Assert.assertEquals(lst, values);
        lst.sort(Comparator.comparingInt(a -> a));
        Assert.assertNotEquals(lst, values);
        CountComparator com = new CountComparator();
        Assert.assertTrue(com.compare(1325939940, -1360544799) > 0);
        Assert.assertTrue(com.compare(2, 1) > 0);
        Assert.assertTrue(com.compare(-3, -2) < 0);
        Assert.assertTrue(com.compare(-2, -3) > 0);
        com.call = 0;
        TreeWorker<List<Integer>> worker = new TreeWorker<>((a, b) -> {
            List<Integer> l = new ArrayList<>();
            int i = 0, j = 0;
            while (i < a.size() && j < b.size()) {
                int aa = a.get(i);
                int bb = b.get(j);
                if (com.compare(aa, bb) < 0) {
                    l.add(aa);
                    i++;
                } else {
                    l.add(bb);
                    j++;
                }
            }
            while (i < a.size()) {
                l.add(a.get(i++));
            }
            while (j < b.size()) {
                l.add(b.get(j++));
            }
            List<Integer> tst = new ArrayList<>(l);
            tst.sort(Integer::compareTo);
            sleepOrThrow(25);
            Assert.assertEquals(tst, l);
            return l;
        }, new Supplier<>() {
            int index;

            @Override
            public List<Integer> get() {
                if (index == values.size()) {
                    return null;
                }
                List<Integer> l = new ArrayList<>();
                l.add(values.get(index++));
                sleepOrThrow(25);
                return l;
            }
        }, null, workers);
        worker.start();
        List<Integer> result = worker.waitToComplete();
        // test O(n log(n))
        Assert.assertTrue(com.call <= count * BitArrayDisk.log2(count));
        Assert.assertEquals(lst, result);
    }

    private static void sleepOrThrow(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            throw new AssertionError("Interruption", e);
        }
    }
}
