package im.unx.perf.mq;

import com.rabbitmq.client.BlockedListener;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.MessageProperties;
import im.unx.perf.config.MqConfig;
import im.unx.perf.util.StrictLogger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeoutException;

/**
 * Created by albert.sun on 9月 24, 2019
 */
public class RabbitMQ extends StrictLogger implements Mq {

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQ.class);

//    ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2,
//            new CustomThreadFactory("rabbitmq"));

    private final Connection connection;

    String queueName = "perf.albert.test";

    public RabbitMQ(MqConfig mqConfig) {

        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(mqConfig.getConn().getHost());
        factory.setPort(mqConfig.getConn().getPort());
        factory.setVirtualHost(mqConfig.getConn().getVhost());
        factory.setUsername(mqConfig.getConn().getUser());
        factory.setPassword(mqConfig.getConn().getPass());

        factory.setAutomaticRecoveryEnabled(true);
        factory.setNetworkRecoveryInterval(10000);
        factory.setRequestedHeartbeat(5);//5s心跳
        factory.setConnectionTimeout(3000);//3000ms 连接超时

        try {
//            this.connection = factory.newConnection(executorService);
            this.connection = factory.newConnection();

            this.connection.addShutdownListener(cause -> {
                String connectionId = this.connection.getId();
                LOGGER.info("conn[{}],close:{}", connectionId, cause.getLocalizedMessage());
            });

            this.connection.addBlockedListener(new BlockedListener() {
                @Override
                public void handleBlocked(String reason) throws IOException {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(reason);
                    }
                }

                @Override
                public void handleUnblocked() throws IOException {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("handleUnblocked");
                    }
                }
            });

        } catch (IOException | TimeoutException e) {

            if (LOGGER.isErrorEnabled()) {
                LOGGER.error("[rabbitmq-conn-error]:{}:{}/{}", factory.getHost(), factory.getPort(), factory.getVirtualHost());
            }

            throw new RuntimeException(e);
        }

        try {
            Channel channel = this.connection.createChannel();
            channel.queueDeclare(queueName, true, false, false, null);
            channel.close();
        } catch (Exception e) {
            logger.error("queueDeclare-error", e);
        }

    }


    Channel newChannel() throws IOException {
        return this.connection.createChannel();
    }

    @Override
    public MqSender createSender() throws IOException {

        return new MqSender() {

            private final Channel channel = newChannel();

            {
                channel.confirmSelect();
            }

            @Override
            public void send(List<String> msgs) {

                for (String msg : msgs) {
                    try {
                        channel.basicPublish("", queueName, MessageProperties.PERSISTENT_TEXT_PLAIN, msg.getBytes());
                    } catch (Exception e) {
                        logger.error("publish-error", e);
                    }
                }

                try {
                    channel.waitForConfirms();
                } catch (Exception e) {
                    logger.error("waitFor-error", e);
                }

            }

            @Override
            public void close() {
                try {
                    channel.close();
                } catch (Exception e) {
                    logger.error("close-error", e);
                }
            }
        };
    }

    @Override
    public MqReceiver createReceiver() {
        return null;
    }


    @Override
    public void close() {
        try {
            this.connection.close();
        } catch (IOException e) {
            // no-OPS
        }
    }


}
