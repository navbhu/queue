package com.buvi.queue;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

abstract class QueueTest {
    QueueService queueService;

    @Test
    void push() {
        long time = System.currentTimeMillis();
        String queueName = "queue-" + time;
        String msg = "hello "+ System.getProperty("line.separator") + "world!!"+ time + System.lineSeparator();
        queueService.push(queueName, msg);
        Message message = queueService.pull(queueName);
        assertEquals(msg, message.getMessage());
    }

    @Test
    void pullFromEmptyQueue() {
        long time = System.currentTimeMillis();
        String queueName = "queue-" + time;
        assertEquals(null, queueService.pull(queueName));
    }

    @Test
    void pull() {
        long time = System.currentTimeMillis();
        String queueName = "queue-" + time;
        String msg = "hello world!!"+ time;
        queueService.push(queueName, msg);
        Message message = queueService.pull(queueName);
        assertEquals(msg, message.getMessage());
    }

    @Test
    void delete() {
        long time = System.currentTimeMillis();
        String queueName = "queue-" + time;
        String msg = "hello world!!"+ time;
        queueService.push(queueName, msg);
        Message message = queueService.pull(queueName);
        assertEquals(msg, message.getMessage());
        queueService.delete(queueName, message.getId());
        assertEquals(null, queueService.pull(queueName));
    }

    @Test
    void  pullAndNotDeleteWithThreadSleep(){
        try {
            long time = System.currentTimeMillis();
            String queueName = "queue-" + time;
            String msg = "hello world!!"+ time;
            queueService.push(queueName, msg);
            Message message = queueService.pull(queueName);
            assertEquals(msg, message.getMessage());
            Thread.sleep(2000);
            message = queueService.pull(queueName);
            assertEquals(null, message);
            Thread.sleep(1100);
            message = queueService.pull(queueName);
            assertEquals(msg, message.getMessage());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    void  pullAndDelete(){
        long time = System.currentTimeMillis();
        String queueName = "queue-" + time;
        String msg = "hello world!!"+ time;
        queueService.push(queueName, msg);
        Message message = queueService.pull(queueName);
        assertEquals(msg, message.getMessage());
        queueService.delete(queueName, message.getId());
        message = queueService.pull(queueName);
        assertEquals(null, message);
        message = queueService.pull(queueName);
        assertEquals(null, message);
    }

    @Test
    void pullMultipleMessages() {
        long time = System.currentTimeMillis();
        String queueName = "queue-" + time;
        String msg = "hello world!! 1";
        queueService.push(queueName, msg);
        String msg1 = "hello world!! 2";
        queueService.push(queueName, msg1);
        List<Message> message = queueService.pull(queueName, 4);
        assertEquals(msg, message.get(0).getMessage());
        assertEquals(msg1, message.get(1).getMessage());
        assertEquals(2, message.size());
    }

}