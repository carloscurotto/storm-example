package ar.com.carloscurotto.storm.complex.transport.activemq;

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.Validate;

import com.google.common.base.Preconditions;

import ar.com.carloscurotto.storm.complex.service.OpenAwareBean;

public class ActiveMQConfiguration extends OpenAwareBean implements Serializable {

    private static final long serialVersionUID = 1L;
    private String brokerUrl;
    private Connection connection;
    private Session session;

    public ActiveMQConfiguration(final String theBrokerUrl) {
        Validate.notBlank(theBrokerUrl, "The broker url cannot be blank.");
        brokerUrl = theBrokerUrl;
    }

    @Override
    protected void doOpen() {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(brokerUrl);
            connection = connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        } catch (Exception exception) {
            throw new RuntimeException("Error while creating and starting the JMS Connection");
        }

    }

    @Override
    protected void doClose() {
        closeSession();
        closeConnection();
    }

    private void closeSession() {
        if (session != null) {
            try {
                session.close();
            } catch (JMSException jmsException) {
            }
        }
    }

    private void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (JMSException jmsException) {
            }
        }
    }

    public Session getSession() {
        Preconditions.checkState(isOpen(), "The session couldn't be created. Try calling open()");
        return session;
    }
}
