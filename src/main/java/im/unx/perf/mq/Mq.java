package im.unx.perf.mq;

import java.io.IOException;

/**
 * Created by albert.sun on 9月 24, 2019
 */
public interface Mq {

    MqSender createSender() throws IOException;

    MqReceiver createReceiver();

    void close();

}
