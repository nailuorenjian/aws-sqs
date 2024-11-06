package com.example.demo.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;

/**
 * @ClassName: AwsSqsController
 * @Description: 采用AWS SDK for Java 2.0 对 [Amazon SQS] 进行编程则更加灵活，可以代替控制台部分的工作。当然也更加繁琐。下面演示代码创建队列和操作消息。
 * @author: 郭秀志 jbcode@126.com
 * @date: 2020/10/26 18:28
 * @Copyright:
 */
@Slf4j
@RestController
@RequestMapping("/aws/sqs")
public class AwsSqsJava2ApiController {


    private static final String QUEUE_NAME = "testQueue_guoxiuzhi_20201027";


    /**
     * 使用AWS的java2新API操作队列，
     * v2版本示例代码：https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/sqs/src/main/java/com/example/sqs
     */
    @Autowired
    private SqsClient sqsClient;

    @GetMapping("/useQueueByJava2Api")
    public void useQueueByJava2Api() {
        // Create a queue
        String queueUrl = createQueue(sqsClient, QUEUE_NAME);

        listQueues(sqsClient);
        listQueuesFilter(sqsClient, queueUrl);

        sendMessage(sqsClient, queueUrl);
        List<Message> messages = receiveMessages(sqsClient, queueUrl);
        changeMessages(sqsClient, queueUrl, messages);
        deleteMessages(sqsClient, queueUrl, messages);
    }

    /**
     * 创建队列，如果存在则不新建。
     *
     * @param sqsClient
     * @param queueName 队列的名字
     * @return
     */
    public static String createQueue(SqsClient sqsClient, String queueName) {

        System.out.println("\nCreate queue");
        // snippet-start:[sqs.java2.sqs_example.create_queue]

        CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                .queueName(queueName)
                .build();

        sqsClient.createQueue(createQueueRequest);
        // snippet-end:[sqs.java2.sqs_example.create_queue]

        System.out.println("\nGet queue URL");
        // snippet-start:[sqs.java2.sqs_example.get_queue]
        GetQueueUrlResponse getQueueUrlResponse =
                sqsClient.getQueueUrl(GetQueueUrlRequest.builder().queueName(queueName).build());
        String queueUrl = getQueueUrlResponse.queueUrl();
        return queueUrl;
        // snippet-end:[sqs.java2.sqs_example.get_queue]
    }

    /**
     * 查询所有的队列
     *
     * @param sqsClient
     */
    public static void listQueues(SqsClient sqsClient) {

        System.out.println("\nList Queues");
        // snippet-start:[sqs.java2.sqs_example.list_queues]
        ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder().build();
        ListQueuesResponse listQueuesResponse = sqsClient.listQueues(listQueuesRequest);
        for (String url : listQueuesResponse.queueUrls()) {
            System.out.println(url);
        }
        // snippet-end:[sqs.java2.sqs_example.list_queues]
    }

    /**
     * 通过队列名的前缀，带条件查询所有的队列。
     *
     * @param sqsClient
     * @param queueUrl
     */
    public static void listQueuesFilter(SqsClient sqsClient, String queueUrl) {
        // List queues with filters
        String namePrefix = "testQueue";
        ListQueuesRequest filterListRequest = ListQueuesRequest.builder()
                .queueNamePrefix(namePrefix).build();

        ListQueuesResponse listQueuesFilteredResponse = sqsClient.listQueues(filterListRequest);
        System.out.println("Queue URLs with prefix: " + namePrefix);
        for (String url : listQueuesFilteredResponse.queueUrls()) {
            System.out.println(url);
        }
        // snippet-end:[sqs.java2.sqs_example.send_message]
    }

    /**
     * 批量发送消息。
     *
     * @param sqsClient
     * @param queueUrl
     */
    public static void sendBatchMessages(SqsClient sqsClient, String queueUrl) {

        System.out.println("\nSend multiple messages");
        // snippet-start:[sqs.java2.sqs_example.send__multiple_messages]
        SendMessageBatchRequest sendMessageBatchRequest = SendMessageBatchRequest.builder()
                .queueUrl(queueUrl)
                .entries(SendMessageBatchRequestEntry.builder().id("id1").messageBody("msg 1").build(),
                        SendMessageBatchRequestEntry.builder().id("id2").messageBody("msg 2").delaySeconds(10).build())
                .build();
        sqsClient.sendMessageBatch(sendMessageBatchRequest);
        // snippet-end:[sqs.java2.sqs_example.send__multiple_messages]
    }

    /**
     * 发送消息
     *
     * @param sqsClient
     * @param queueUrl
     */
    public static void sendMessage(SqsClient sqsClient, String queueUrl) {
        System.out.println("\nSend message");
        // snippet-start:[sqs.java2.sqs_example.send_message]
        sqsClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody("Hello world from guoxiuzhi!")
                .delaySeconds(10) //发送延迟10秒的消息，消费者不能立即可见此消息进行消费。
                .build());
        // snippet-end:[sqs.java2.sqs_example.send_message]
    }

    /**
     * 消费消息。
     *
     * @param sqsClient
     * @param queueUrl
     * @return
     */
    public static List<Message> receiveMessages(SqsClient sqsClient, String queueUrl) {

        System.out.println("\nReceive messages");
        // snippet-start:[sqs.java2.sqs_example.retrieve_messages]
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(5)
                .build();
        List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
        for (Message message : messages) {
            System.out.println("message.body() = " + message.body());
        }
        return messages;
        // snippet-end:[sqs.java2.sqs_example.retrieve_messages]
    }

    /**
     * 改变消息的可见性超时，即这段时间内一个消费者进行消费时其他消费者对消息不可见。
     *
     * @param sqsClient
     * @param queueUrl
     * @param messages
     */
    public static void changeMessages(SqsClient sqsClient, String queueUrl, List<Message> messages) {

        System.out.println("\nChange message visibility");
        for (Message message : messages) {
            ChangeMessageVisibilityRequest req = ChangeMessageVisibilityRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .visibilityTimeout(100)
                    .build();
            sqsClient.changeMessageVisibility(req);
        }

    }

    /**
     * 删除消息
     *
     * @param sqsClient
     * @param queueUrl
     * @param messages
     */
    public static void deleteMessages(SqsClient sqsClient, String queueUrl, List<Message> messages) {
        System.out.println("\nDelete messages");
        // snippet-start:[sqs.java2.sqs_example.delete_message]
        for (Message message : messages) {
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .receiptHandle(message.receiptHandle())
                    .build();
            sqsClient.deleteMessage(deleteMessageRequest);
        }
        // snippet-end:[sqs.java2.sqs_example.delete_message]
    }
}
