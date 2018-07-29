package com.buvi.queue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class InMemoryQueueTest extends QueueTest {


    @BeforeEach
    void setUp() {
        queueService = QueueFactory.getInMemoryQueue();
    }

    @AfterEach
    void tearDown() {
    }

    @Test
    void  pullAndNotDeleteChangingVisibility(){
        long time = System.currentTimeMillis();
        String queueName = "queue-" + time;
        String msg = "hello world!!"+ time;
        queueService.push(queueName, msg);
        Message message = queueService.pull(queueName);
        assertEquals(msg, message.getMessage());
        assertEquals(null, queueService.pull(queueName));
        message.setVisibilityTimeout(System.currentTimeMillis());
        message = queueService.pull(queueName);
        assertEquals(msg, message.getMessage());
    }
}