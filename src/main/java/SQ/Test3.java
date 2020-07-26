package SQ;

import java.lang.reflect.Field;
import java.util.concurrent.locks.LockSupport;

public class Test3 {

    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {

        Thread thread1 = new Thread();
        Thread thread2 = new Thread();
        Thread thread3 = new Thread();

        Runnable runnable1 = () -> {
            while (true) {
                System.out.println("==");
                System.out.println("阿");
                LockSupport.unpark(thread2);
                LockSupport.park();
            }
        };
        Runnable runnable2 = () -> {
            while (true) {
                LockSupport.park();
                System.out.println("里");
                LockSupport.unpark(thread3);
            }
        };
        Runnable runnable3 = () -> {
            while (true) {
                LockSupport.park();
                System.out.println("巴巴");
                LockSupport.unpark(thread1);
            }
        };

        Field target = Thread.class.getDeclaredField("target");
        target.setAccessible(true);

        target.set(thread1, runnable1);
        target.set(thread2, runnable2);
        target.set(thread3, runnable3);

        thread1.start();
        thread2.start();
        thread3.start();
    }

}
