package com.rhy.rocketmqdemo;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.spring.core.RocketMQTemplate;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class RocketmqDemoApplicationTests {
    @Autowired
    RocketMQTemplate rocketMQTemplate;
    @Test
    void simpleProducer() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        DefaultMQProducer producer = rocketMQTemplate.getProducer();
        for (int i = 0; i < 100; i++) {
            Message msg = new Message(
                    "TopicTest",
                    "TagA",
                    "Hello world".getBytes(StandardCharsets.UTF_8)
            );
            SendResult send = rocketMQTemplate.getProducer().send(msg);
            System.out.printf("%s%n", send);
        }

    }
    @Test
    void simpleAsynProducer() throws MQBrokerException, RemotingException, InterruptedException, MQClientException {
        DefaultMQProducer producer = rocketMQTemplate.getProducer();

        int messageCount = 100;
        final CountDownLatch countDownLatch = new CountDownLatch(messageCount);
        for (int i = 0; i < messageCount; i++) {
            try {
                final int index = i;
                Message msg = new Message("TopicTest",
                        "TagA",
                        "OrderID"+index,
                        "Hello world".getBytes(RemotingHelper.DEFAULT_CHARSET));
                producer.send(msg, new SendCallback() {
                    @Override
                    public void onSuccess(SendResult sendResult) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d OK %s %n", index, sendResult.getMsgId());
                    }

                    @Override
                    public void onException(Throwable e) {
                        countDownLatch.countDown();
                        System.out.printf("%-10d Exception %s %n", index, e);
                        e.printStackTrace();
                    }
                });
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        countDownLatch.await(5, TimeUnit.SECONDS);
        producer.shutdown();
    }



}
