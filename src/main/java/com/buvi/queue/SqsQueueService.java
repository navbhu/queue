package com.buvi.queue;

import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.ReceiveMessageResult;

import java.util.List;

public class SqsQueueService implements QueueService {

  private AmazonSQSClient sqsClient;

  public SqsQueueService(AmazonSQSClient sqsClient) {
    this.sqsClient = sqsClient;
  }

  @Override
  public void push(String queueName, String message) {
    sqsClient.sendMessage(queueName, message);
  }

  @Override
  public Message pull(String queueName) {
    Message message = null;
    ReceiveMessageRequest request = new ReceiveMessageRequest();
    request.setMaxNumberOfMessages(1);
    ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(request.withQueueUrl(queueName));
    List<com.amazonaws.services.sqs.model.Message> messageList = receiveMessageResult.getMessages();
    for (com.amazonaws.services.sqs.model.Message sqsMessage : messageList) {
      message = new Message(sqsMessage.getMessageId(), sqsMessage.getBody());
      break;
    }
    return message;
  }

  @Override
  public void delete(String queueName, String messageId) {
    sqsClient.deleteMessage(queueName, messageId);
  }

  @Override
  public List<Message> pull(String queueName, int limit) {
    return null;
  }

}
