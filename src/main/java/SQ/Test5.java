package SQ;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Test5 {

    private static Integer flag = 0;

    /**
     * 对 Test2 的修改，悲观锁
     */
    public static void main(String[] args) {
        Lock lock = new ReentrantLock();
        Condition condition = lock.newCondition();

        Thread thread1 = new Thread(() -> {
            while (true) {
                try {
                    lock.lock();
                    while (flag != 0) {
                        condition.await();
                    }
                    System.out.println("==");
                    System.out.println("阿");
                    flag = 1;
                    condition.signalAll();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }
        }, "t1");

        Thread thread2 = new Thread(() -> {
            while (true) {
                try {
                    lock.lock();
                    while (flag != 1) {
                        condition.await();
                    }
                    System.out.println("里");
                    flag = 2;
                    condition.signalAll();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }
        }, "t2");

        Thread thread3 = new Thread(() -> {
            while (true) {
                try {
                    lock.lock();
                    while (flag != 2) {
                        condition.await();
                    }
                    System.out.println("巴巴");
                    flag = 0;
                    condition.signalAll();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }
        }, "t3");

        thread1.start();
        thread2.start();
        thread3.start();
    }

}
