package ourchem.activemq.durable;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

/**
 * 持久化Topic生产者
 */
public class JmsProducer_Topic_Persist {
    private static final String ACTIVEMQ_URL = "tcp://192.168.106.128:61616";
    private static final String ACTIVEMQ_TOPIC_NAME = "Topic-Persist";

    public static void main(String[] args) throws JMSException {
        //1.创建连接工厂，按照给定的URL，采用默认的用户名密码
        ActiveMQConnectionFactory activeMQConnectionFactory = new ActiveMQConnectionFactory(ACTIVEMQ_URL);
        //2.通过连接工厂,持久化的topic必须在生产者创建并设置持久化完成后调用start
        Connection connection = activeMQConnectionFactory.createConnection();
        //3.创建会话session
        //两个参数transacted=事务,acknowledgeMode=确认模式(签收)
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        //4.创建目的地(具体是队列queue还是主题topic)
        Topic topic = session.createTopic(ACTIVEMQ_TOPIC_NAME);
        //5.创建消息的生产者
        MessageProducer messageProducer = session.createProducer(topic);
        //6.设置生产者生产持久化的Topic
        messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
        //7.启动连接
        connection.start();
        //8.通过使用持久化Topic消息生产者,生产三条消息,发送到MQ的队列里面
        for (int i = 0; i < 3; i++) {
            //7.通过session创建消息
            TextMessage textMessage = session.createTextMessage("msg-persist" + i);
            //8.使用指定好目的地的消息生产者发送消息
            messageProducer.send(textMessage);
        }
        //9.关闭资源
        messageProducer.close();
        session.close();
        connection.close();
        System.out.println("****TOPIC_NAME消息发布到MQ完成");
    }

}


