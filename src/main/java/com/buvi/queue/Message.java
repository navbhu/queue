package com.buvi.queue;

public class Message {

    private final String message;
    private final String id;
    private long visibilityTimeout;

    public Message(String id, String message){
        this.message = message;
        this.id = id;
    }

    public String getMessage() {
        return message;
    }

    public String getId() {
        return id;
    }

    public long getVisibilityTimeout() {
        return visibilityTimeout;
    }

    void setVisibilityTimeout(long visibilityTimeout) {
        this.visibilityTimeout = visibilityTimeout;
    }

    @Override
    public String toString() {
        return getId() + ":" + getMessage() +":"+ getVisibilityTimeout();
    }
}