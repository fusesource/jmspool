/**
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 *
 **/
package org.fusesource.jms.xapool;

import java.io.Serializable;
import javax.jms.*;
import javax.transaction.xa.XAResource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.geronimo.transaction.manager.WrapperNamedXAResource;


public class PooledSession implements TopicSession, QueueSession, XASession {

    private static final Log log = LogFactory.getLog(PooledSession.class);

    private XASession session;
    private ObjectPool sessionPool;
    private MessageProducer messageProducer;
    private QueueSender queueSender;
    private TopicPublisher topicPublisher;
    private boolean ignoreClose;
    private String name = null;

    public PooledSession(final XASession session, final ObjectPool sessionPool) {
        this.session = session;
        this.sessionPool = sessionPool;
        this.ignoreClose = false;
    }

    protected void setName(String newName) {
        this.name = newName;
    }

    public String getName() {
        return name;
    }

    public boolean getIgnoreClose() {
        return this.ignoreClose;
    }

    public void setIgnoreClose(final boolean ignoreClose) {
        this.ignoreClose = ignoreClose;
    }

    /**
     * If the Session goes into an unstable (unusable) state, then we want to
     * close it down and permanently remove it from the pool.
     */
    public void destroyAndRemoveFromPool() {
        try {
            sessionPool.invalidateObject(this);
        } catch (Throwable t) {
            log.warn("Unable to remove invalidated JMS Session from the pool due to the following exception.  Will ignore the exception and continue.", t);
        }
    }


    public void close() throws JMSException {
        if (log.isDebugEnabled()) log.debug("---->>>>> PooledSpringXASession.close() called");
        // If we are associated with a transaction, then we will let
        // PooledSpringXAConnection's transaction synchronization handle closing
        // us at the end of the transaction.
        if (!getIgnoreClose()) {
            if (log.isDebugEnabled()) log.debug("---->>>>> ignoreClose = false, so returning session pool...");
            // TODO a cleaner way to reset??

            // lets reset the session
            getActualSession().setMessageListener(null);

            try {
                sessionPool.returnObject(this);
            } catch (Exception e) {
                final JMSException jmsException = new JMSException("Failed to return session to pool: " + e);
                jmsException.initCause(e);
                throw jmsException;
            }
        } else if (log.isDebugEnabled()) {
            log.debug("---->>>>> ignoreClose IS TRUE!  KEEPING SESSION OPEN!");
        }
    }

    public void commit() throws JMSException {
        throw new JMSException("Cannot commit() inside XASession");
    }

    public BytesMessage createBytesMessage() throws JMSException {
        return getActualSession().createBytesMessage();
    }

    public MapMessage createMapMessage() throws JMSException {
        return getActualSession().createMapMessage();
    }

    public Message createMessage() throws JMSException {
        return getActualSession().createMessage();
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        return getActualSession().createObjectMessage();
    }

    public ObjectMessage createObjectMessage(Serializable serializable)
            throws JMSException {
        return getActualSession().createObjectMessage(serializable);
    }

    public Queue createQueue(String s) throws JMSException {
        return getActualSession().createQueue(s);
    }

    public StreamMessage createStreamMessage() throws JMSException {
        return getActualSession().createStreamMessage();
    }

    public TemporaryQueue createTemporaryQueue() throws JMSException {
        return getActualSession().createTemporaryQueue();
    }

    public TemporaryTopic createTemporaryTopic() throws JMSException {
        return getActualSession().createTemporaryTopic();
    }

    public void unsubscribe(String s) throws JMSException {
        getActualSession().unsubscribe(s);
    }

    public TextMessage createTextMessage() throws JMSException {
        return getActualSession().createTextMessage();
    }

    public TextMessage createTextMessage(String s) throws JMSException {
        return getActualSession().createTextMessage(s);
    }

    public Topic createTopic(String s) throws JMSException {
        return getActualSession().createTopic(s);
    }

    public int getAcknowledgeMode() throws JMSException {
        return getActualSession().getAcknowledgeMode();
    }

    public boolean getTransacted() throws JMSException {
        return true;
    }

    public void recover() throws JMSException {
        getActualSession().recover();
    }

    public void rollback() throws JMSException {
        throw new JMSException("Cannot rollback() inside XASession");
    }

    public void run() {
        if (session != null) {
            session.run();
        }
    }

    public XAResource getXAResource() {
        try {

            XAResource xaRes = getActualSession().getXAResource();
            if (name != null) {
                xaRes = new WrapperNamedXAResource(xaRes, name);
            }
            return xaRes;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public Session getSession() throws JMSException {
        return this;
    }


    // Consumer related methods
    //-------------------------------------------------------------------------
    public QueueBrowser createBrowser(Queue queue) throws JMSException {
        return getActualSession().createBrowser(queue);
    }

    public QueueBrowser createBrowser(Queue queue, String selector)
            throws JMSException {
        return getActualSession().createBrowser(queue, selector);
    }

    public MessageConsumer createConsumer(Destination destination)
            throws JMSException {
        return getActualSession().createConsumer(destination);
    }

    public MessageConsumer createConsumer(Destination destination,
                                          String selector) throws JMSException {
        return getActualSession().createConsumer(destination, selector);
    }

    public MessageConsumer createConsumer(Destination destination,
                                          String selector, boolean noLocal)
            throws JMSException {
        return getActualSession().createConsumer(destination, selector, noLocal);
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String selector)
            throws JMSException {
        return getActualSession().createDurableSubscriber(topic, selector);
    }

    public TopicSubscriber createDurableSubscriber(Topic topic, String name,
                                                   String selector,
                                                   boolean noLocal)
            throws JMSException {
        return getActualSession().createDurableSubscriber(topic, name, selector, noLocal);
    }

    public MessageListener getMessageListener() throws JMSException {
        return getActualSession().getMessageListener();
    }

    public void setMessageListener(MessageListener messageListener)
            throws JMSException {
        getActualSession().setMessageListener(messageListener);
    }

    public TopicSubscriber createSubscriber(Topic topic) throws JMSException {
        return ((TopicSession) getActualSession()).createSubscriber(topic);
    }

    public TopicSubscriber createSubscriber(Topic topic, String selector,
                                            boolean local) throws JMSException {
        return ((TopicSession) getActualSession()).createSubscriber(topic, selector, local);
    }

    public QueueReceiver createReceiver(Queue queue) throws JMSException {
        return ((QueueSession) getActualSession()).createReceiver(queue);
    }

    public QueueReceiver createReceiver(Queue queue, String selector)
            throws JMSException {
        return ((QueueSession) getActualSession()).createReceiver(queue, selector);
    }


    // Producer related methods
    //-------------------------------------------------------------------------
    public MessageProducer createProducer(Destination destination)
            throws JMSException {
        return new PooledProducer(getMessageProducer(), destination);
    }

    public QueueSender createSender(Queue queue) throws JMSException {
        return new PooledQueueSender(getQueueSender(), queue);
    }

    public TopicPublisher createPublisher(Topic topic) throws JMSException {
        return new PooledTopicPublisher(getTopicPublisher(), topic);
    }

    // Implementation methods
    //-------------------------------------------------------------------------
    public XASession getActualSession() throws JMSException {
        if (this.session == null) {
            throw new JMSException("The session has already been closed");
        }
        return this.session;
    }

    public MessageProducer getMessageProducer() throws JMSException {
        if (this.messageProducer == null) {
            this.messageProducer = getActualSession().createProducer(null);
        }
        return this.messageProducer;
    }

    public QueueSender getQueueSender() throws JMSException {
        if (this.queueSender == null) {
            this.queueSender = ((QueueSession) getActualSession()).createSender(null);
        }
        return this.queueSender;
    }

    public TopicPublisher getTopicPublisher() throws JMSException {
        if (this.topicPublisher == null) {
            this.topicPublisher = ((TopicSession) getActualSession()).createPublisher(null);
        }
        return this.topicPublisher;
    }

}
