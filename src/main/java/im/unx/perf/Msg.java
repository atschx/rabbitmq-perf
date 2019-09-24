package im.unx.perf;

/**
 * Created by albert.sun on 9月 24, 2019
 */
public class Msg {

    private static int timestampLength = 13;

    public static String prefix(int msgSize) {
        int prefixLength = msgSize - timestampLength;
        String prefix = "albert";
        if (prefixLength > 0) {
            for (int i = 0; i < prefixLength; i++) {
                prefix = prefix.concat("0");
            }
        }
        return prefix;
    }

    public static String addTimestamp(String prefix) {
        return prefix + System.currentTimeMillis();
    }

    public static long extractTimestamp(String msg) {
        return Long.valueOf(msg.substring(msg.lastIndexOf('@') + 1));
    }

}
