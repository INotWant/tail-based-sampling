package SQ;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Test2 {

    /**
     * ERROR：signalAll 会丢失
     */

    private static Lock lock = new ReentrantLock();
    private static Condition condition1 = lock.newCondition();
    private static Condition condition2 = lock.newCondition();
    private static Condition condition3 = lock.newCondition();

    public static void main(String[] args) {
        new Thread(() -> {
            while (true) {
                try {
                    lock.lock();
                    System.out.println("==");
                    System.out.println("阿");
                    condition1.signalAll();
                    condition3.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }
        }).start();

        new Thread(() -> {
            while (true) {
                try {
                    lock.lock();
                    condition1.await();
                    System.out.println("里");
                    condition2.signalAll();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }
        }).start();

        new Thread(() -> {
            while (true) {
                try {
                    lock.lock();
                    condition2.await();
                    System.out.println("巴巴");
                    condition3.signalAll();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    lock.unlock();
                }
            }
        }).start();
    }

}
