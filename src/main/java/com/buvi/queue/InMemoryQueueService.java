package com.buvi.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class InMemoryQueueService implements QueueService {

    private static final Map<String, InMemoryQueue> MAP = new ConcurrentHashMap<>();
    private static final long TIME = System.currentTimeMillis();
    private static final Message EMPTY_MSG = new Message("","");
    private static final QueueService INSTANCE = new InMemoryQueueService();
    private static final long DEFAULT_VISIBILITY = 3000L;

    private InMemoryQueueService(){
    }

    private InMemoryQueue getOrCreateQueue(String queueName) {
        MAP.putIfAbsent(queueName, new InMemoryQueue());
        return MAP.get(queueName);
    }

    @Override
    public void push(String queueName, String message) {
        InMemoryQueue inMemoryQueue = getOrCreateQueue(queueName);
        inMemoryQueue.push(message);
    }

    @Override
    public Message pull(String queueName) {
        InMemoryQueue inMemoryQueue = getOrCreateQueue(queueName);
        return inMemoryQueue.pull();
    }

    @Override
    public void delete(String queueName, String messageId) {
        InMemoryQueue inMemoryQueue = getOrCreateQueue(queueName);
        inMemoryQueue.delete(messageId);
    }

    @Override
    public List<Message> pull(String queueName, int limit) {
        InMemoryQueue inMemoryQueue = getOrCreateQueue(queueName);
        return inMemoryQueue.pull(limit);
    }

    static QueueService getInstance(){
        return INSTANCE;
    }

    private class InMemoryQueue {
        private LinkedBlockingQueue<Message> queue = new LinkedBlockingQueue<>();
        private LinkedBlockingQueue<Message> consumedMessages = new LinkedBlockingQueue<>();
        private ConcurrentHashMap<String, Message> deletedMessages = new ConcurrentHashMap<>();
        private AtomicLong counter = new AtomicLong(0L);


        private void push(String message){
            queue.add(new Message(TIME+"-"+ String.format("%015d", counter.getAndIncrement()), message));
        }

        private Message pull() {
            Message message;

            synchronized (this) {
                message = consumedMessages.peek();

                while (message != null) {
                    if (deletedMessages.containsKey(message.getId())) {
                        consumedMessages.remove();
                        deletedMessages.remove(message.getId());
                        message = consumedMessages.peek();
                    } else {
                        if (message.getVisibilityTimeout() > System.currentTimeMillis()) {
                            message = null;
                        } else {
                            message = consumedMessages.poll();
                            break;
                        }
                    }
                }
            }

            if(message == null){
                message = queue.poll();
            }

            addToRetrievedMessage(message);

            return message;
        }


        private List<Message> pull(int limit) {
            List<Message> messageList = new ArrayList<>(limit);

            Message message;
            int i  = 0;
            synchronized (this) {
                message = consumedMessages.peek();

                while (message != null && i < limit) {
                    if (deletedMessages.containsKey(message.getId())) {
                        consumedMessages.remove();
                        deletedMessages.remove(message.getId());
                        message = consumedMessages.peek();
                    } else {
                        if (message.getVisibilityTimeout() > System.currentTimeMillis()) {
                            message = null;
                        } else {
                            message = consumedMessages.poll();
                            messageList.add(message);
                            addToRetrievedMessage(message);
                            i++;
                            message = consumedMessages.peek();
                            if(i == limit) {
                                break;
                            }
                        }
                    }
                }
            }

            if (message == null) {
                message = queue.poll();
                while (message != null && i < limit) {
                    messageList.add(message);
                    addToRetrievedMessage(message);
                    i++;
                    message = queue.poll();
                }
            }

            return messageList;
        }

        private void addToRetrievedMessage(Message message){
            if(message != null) {
                message.setVisibilityTimeout(System.currentTimeMillis() + DEFAULT_VISIBILITY);
                consumedMessages.add(message);
            }
        }

        private void delete(String messageId){
            deletedMessages.putIfAbsent(messageId, EMPTY_MSG);
        }

    }
}