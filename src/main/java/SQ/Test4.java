package SQ;

import java.util.concurrent.atomic.AtomicInteger;

public class Test4 {

    /**
     * 对 Test2 的修改，乐观锁
     */
    public static void main(String[] args) {
        AtomicInteger atomicInteger = new AtomicInteger(0);

        Thread thread1 = new Thread(() -> {
            while (true) {
                while (atomicInteger.get() != 0) {
                    // 忙等
                }
                System.out.println("==");
                System.out.println("阿");
                atomicInteger.incrementAndGet(); // 1
            }
        });

        Thread thread2 = new Thread(() -> {
            while (true) {
                while (atomicInteger.get() != 1) {
                    // 忙等
                }
                System.out.println("里");
                atomicInteger.incrementAndGet(); // 2
            }
        });

        Thread thread3 = new Thread(() -> {
            while (true) {
                while (atomicInteger.get() != 2) {
                    // 忙等
                }
                System.out.println("巴巴");
                atomicInteger.set(0);
            }
        });

        thread1.start();
        thread2.start();
        thread3.start();
    }

}
