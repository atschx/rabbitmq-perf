package im.unx.perf.mq;

import java.util.List;

/**
 * Created by albert.sun on 9月 24, 2019
 */
public interface MqSender {

    void send(List<String> msgs);

    void close();

}
