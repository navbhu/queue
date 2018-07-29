package com.buvi.queue;

import com.amazonaws.services.sqs.AmazonSQSClient;

public class QueueFactory {

    public static QueueService getInMemoryQueue(){
        return InMemoryQueueService.getInstance();
    }

    public static QueueService getFileQueue(){
        return FileQueueService.getInstance();
    }

    public static QueueService getSqsQueue(){
        return new SqsQueueService(new AmazonSQSClient());
    }
}