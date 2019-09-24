package im.unx.perf.util;

/**
 * Created by albert.sun on 9月 24, 2019
 */
public interface Clock {

    default long currentTimeMills() {
        return System.currentTimeMillis();
    }

    default long nanoTime() {
        return System.nanoTime();
    }

}
