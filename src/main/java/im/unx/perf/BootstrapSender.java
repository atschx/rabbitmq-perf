package im.unx.perf;

import im.unx.perf.config.MqConfig;
import im.unx.perf.mq.Mq;
import im.unx.perf.mq.MqSender;
import im.unx.perf.mq.RabbitMQ;
import im.unx.perf.util.Clock;
import im.unx.perf.util.CustomThreadFactory;
import im.unx.perf.util.StrictLogger;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import io.prometheus.client.exporter.HTTPServer;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by albert.sun on 9月 24, 2019
 */
public class BootstrapSender {

    private static MqConfig mqConfig;

    public static void main(String[] args) {

        Yaml yaml = new Yaml();

        try {
            mqConfig = yaml.loadAs(new FileInputStream("conf/mqconfig.yaml"), MqConfig.class);
        } catch (FileNotFoundException e) {
            throw new RuntimeException("检查配置文件", e);
        }

        Mq rabbitMQ = new RabbitMQ(mqConfig);

        Counter counter = Counter.build("_sent_total", "number of sent messages").labelNames("counter").register(CollectorRegistry.defaultRegistry);

        Histogram histogram = Histogram.build("_send_latency_ms", "latency of sent messages")
                .buckets(0, 50, 100, 150, 200, 250, 300, 350, 400, 450, 500, 600, 700, 800, 900,
                        1000, 1250, 1500, 1750,
                        2000, 2500, 3000, 3500, 4000, 4500, 5000)
                .labelNames("latency").register(CollectorRegistry.defaultRegistry);

        Gauge gauge = Gauge.build("_sent_threads_done", "number of sent threads done").labelNames("run").register(CollectorRegistry.defaultRegistry);

        SenderRunnable senderRunnable = new SenderRunnable(rabbitMQ, Msg.prefix(1000), 1000, 1,
                counter.labels("counter"),
                histogram.labels("latency"),
                gauge.labels("run"),
                new Clock() {
                });

        HTTPServer httpServer = null;
        try {
            httpServer = new HTTPServer(9999);
        } catch (IOException e) {
            throw new RuntimeException("监控网关启动失败", e);
        }

        // 目前开发环境测试发现一个Connection 最大channel数为2047

        int threadCount = 2047;

        CustomThreadFactory customThreadFactory = new CustomThreadFactory("rabbitmq-perf");
        List<Thread> threads = new ArrayList<>(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Thread thread = customThreadFactory.newThread(senderRunnable);
            threads.add(thread);
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                thread.interrupt();
            }
        }

        rabbitMQ.close();
        httpServer.stop();
    }


    public static class SenderRunnable extends StrictLogger implements Runnable {

        private final Mq mq;
        private final String msgPrefix;
        private final int msgCount;
        private final int maxSendMsgBatchSize;
        private final Clock clock;
        private final Counter.Child senderCounter;
        private final Histogram.Child sendLatency;
        private final Gauge.Child senderDone;

        public SenderRunnable(Mq mq,
                              String msgPrefix, int msgCount, int maxSendMsgBatchSize,
                              Counter.Child senderCounter,
                              Histogram.Child sendLatency,
                              Gauge.Child senderDone,
                              Clock clock) {
            this.mq = mq;
            this.msgPrefix = msgPrefix;
            this.msgCount = msgCount;
            this.maxSendMsgBatchSize = maxSendMsgBatchSize;
            this.senderCounter = senderCounter;
            this.sendLatency = sendLatency;
            this.senderDone = senderDone;
            this.clock = clock;
        }

        @Override
        public void run() {

            MqSender mqSender = null;
            try {
                mqSender = mq.createSender();
            } catch (IOException e) {
                e.printStackTrace();
            }

            try {
                int leftToSend = msgCount;
                logger.info("Sending {} messages", leftToSend);
                while (leftToSend > 0) {

                    int batchSize = Math.min(leftToSend, new Random().nextInt(maxSendMsgBatchSize) + 1);

                    String fullMsg = Msg.addTimestamp(msgPrefix + "@");
                    List msgs = new ArrayList(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        msgs.add(fullMsg);
                    }

                    Long start = clock.currentTimeMills();
                    mqSender.send(msgs);
                    sendLatency.observe(clock.currentTimeMills() - start);
                    leftToSend = leftToSend - batchSize;
                    senderCounter.inc(batchSize);
                }
                senderDone.inc();
            } catch (Exception e) {
                logger.error("send-ERROR", e);
            } finally {
                mqSender.close();
            }

        }
    }
}
