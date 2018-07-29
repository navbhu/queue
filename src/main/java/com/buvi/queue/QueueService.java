package com.buvi.queue;

import java.util.List;

public interface QueueService {

    public void push(String queueName, String message);
    public Message pull(String queueName);
    public void delete(String queueName, String messageId);
    public List<Message> pull(String queueName, int limit);

}
