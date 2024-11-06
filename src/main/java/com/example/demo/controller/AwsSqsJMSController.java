package com.example.demo.controller;

import com.amazon.sqs.javamessaging.message.SQSTextMessage;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jms.annotation.JmsListener;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.jms.JMSException;
import javax.jms.Message;

/**
 * @ClassName: AwsSqsController
 * @Description: 通过JMS方式发送和消费消息。
 * @author: 郭秀志 jbcode@126.com
 * @date: 2020/10/27 21:28
 * @Copyright:
 */
@Slf4j
@RestController
@RequestMapping("/aws/sqs")
public class AwsSqsJMSController {

    /**
     * 第一种方式使用标准的JMS，简单
     */
    @Autowired
    private JmsTemplate jmsTemplate;

    /**
     * 发送消息
     *
     * @param message
     */
    @GetMapping("/send")
    public void oeSend(String message) {
        jmsTemplate.convertAndSend("NPay-Queue", message);
    }

    /**
     * 接收消息
     * destination  队列名称
     *
     * @param message
     * @throws JMSException 我只创建了一个listenerFactory 这里会默认使用那一个，如果有多个Factory 需要手动指定
     */
    @JmsListener(destination = "NPay-Queue")
    public void oeListener(Message message) throws JMSException {
        SQSTextMessage textMessage = (SQSTextMessage) message;
        System.out.println("收到一条消息" + textMessage.getText());

        //如果设置的是客户端确认模式(Session.CLIENT_ACKNOWLEDGE)，所以要记得调用acknowledge()删除sqs消息。
        message.acknowledge();
    }

}