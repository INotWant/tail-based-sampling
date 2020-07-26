package SQ;

import java.util.concurrent.SynchronousQueue;

/**
 * 目标：实现三个线程的依次打印。
 *      比如，第一个线程打印 "阿"，第二个线程打印 "里"，第三个线程打印 "巴巴"
 */
public class Test1 {

    private static SynchronousQueue<Boolean> synchronousQueue1 = new SynchronousQueue<>();
    private static SynchronousQueue<Boolean> synchronousQueue2 = new SynchronousQueue<>();
    private static SynchronousQueue<Boolean> synchronousQueue3 = new SynchronousQueue<>();

    public static void main(String[] args) throws InterruptedException {

        new Thread(() -> {
            try {
                while (true) {
                    synchronousQueue3.take();
                    System.out.println("===");
                    System.out.println("阿");
                    synchronousQueue1.put(true);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                while (true) {
                    synchronousQueue1.take();
                    System.out.println("里");
                    synchronousQueue2.put(true);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        new Thread(() -> {
            try {
                while (true) {
                    synchronousQueue2.take();
                    System.out.println("巴巴");
                    synchronousQueue3.put(true);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }).start();

        synchronousQueue3.put(true);
    }

}
