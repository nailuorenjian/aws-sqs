package com.example.demo.config;

import com.amazon.sqs.javamessaging.ProviderConfiguration;
import com.amazon.sqs.javamessaging.SQSConnectionFactory;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jms.annotation.EnableJms;
import org.springframework.jms.config.DefaultJmsListenerContainerFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.support.destination.DynamicDestinationResolver;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.SqsClientBuilder;

import javax.jms.Session;

/**
 * @ClassName: JmsConfig
 * @Description: 通过JMS操作AWS SQS队列的配置类
 * @author: 郭秀志 jbcode@126.com
 * @date: 2020/10/27 19:17
 * @Copyright:
 */
@Configuration
@EnableJms
public class JmsConfig {

    @Value("${aws.accessKey}")
    private String accessKey;

    @Value("${aws.secretKey}")
    private String secretKey;

    @Value("${aws.region}")
    private String region;

    SQSConnectionFactory connectionFactory = null;

    AmazonSQSClientBuilder amazonSQSClientBuilder = null;


    /**
     * 使用aws-doc-sdk-examples/javav2版本代码，操作sqs的client。
     * https://github.com/awsdocs/aws-doc-sdk-examples/blob/master/javav2/example_code/sqs/src/main/java/com/example/sqs/SQSExample.java
     *
     * @return
     */
    @Bean
    public SqsClient getSqsClient() {
        SqsClientBuilder sqsClientBuilder = SqsClient.builder().
                region(Region.of(region)).credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("" +
                        accessKey, secretKey)));
        return sqsClientBuilder.build();
    }

    /**
     * 组装client builder
     *
     * @return
     */
    public AmazonSQSClientBuilder getAmazonSQSClientBuilder() {
        if (amazonSQSClientBuilder != null) {
            return amazonSQSClientBuilder;
        }
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials(accessKey, secretKey);
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setConnectionTimeout(3000);
        clientConfiguration.setProtocol(Protocol.HTTP);
        clientConfiguration.useGzip();
        clientConfiguration.useTcpKeepAlive();
        AmazonSQSClientBuilder amazonSQSClientBuilder = AmazonSQSClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withClientConfiguration(clientConfiguration)
                .withRegion(region);
        return amazonSQSClientBuilder;
    }

    /**
     * 构建ConnectionFactory，来构造DefaultJmsListenerContainerFactory。
     *
     * @return
     */
    public SQSConnectionFactory getConnectionFactory() {
        if (connectionFactory != null) {
            return connectionFactory;
        }

        connectionFactory = new SQSConnectionFactory(new ProviderConfiguration(), getAmazonSQSClientBuilder());
        return connectionFactory;
    }

    /**
     * 核心配置方法，返回自定义的DefaultJmsListenerContainerFactory。
     *
     * @return
     */
    @Bean
    public DefaultJmsListenerContainerFactory jmsListenerContainerFactory() {
        DefaultJmsListenerContainerFactory factory = new DefaultJmsListenerContainerFactory();
        factory.setConnectionFactory(this.getConnectionFactory());
        factory.setDestinationResolver(new DynamicDestinationResolver());
        factory.setConcurrency("3-10");
        /**
         *  SESSION_TRANSACTED
         *  CLIENT_ACKNOWLEDGE : After the client confirms, the client must call the acknowledge method of javax.jms.message after receiving the message. After the confirmation, the JMS server will delete the message
         *  AUTO_ACKNOWLEDGE : Automatic acknowledgment, no extra work required for client to send and receive messages
         *  DUPS_OK_ACKNOWLEDGE : Allow the confirmation mode of the replica. Once a method call from the receiving application returns from the processing message, the session object acknowledges the receipt of the message; and duplicate acknowledgments are allowed. This pattern is very effective when resource usage needs to be considered.
         */
        factory.setSessionAcknowledgeMode(Session.AUTO_ACKNOWLEDGE);
        factory.setSessionTransacted(false);
        return factory;
    }

    /**
     * 构建自己的JmsTemplate，在后续的Service或者Controller里面通过自动装配调用发送消息等操作。
     *
     * @return
     */
    @Bean
    public JmsTemplate defaultJmsTemplate() {
        return new JmsTemplate(this.getConnectionFactory());
    }
}
