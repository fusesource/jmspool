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

import javax.jms.ConnectionConsumer;
import javax.jms.ConnectionMetaData;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.ServerSessionPool;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.transaction.RollbackException;
import javax.transaction.Status;
import javax.transaction.SystemException;
import javax.transaction.TransactionManager;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class PooledXAConnection implements TopicConnection, QueueConnection, XAConnection {

    private static final Log log = LogFactory.getLog(PooledXAConnection.class);

    private final ConnectionInfo connectionInfo;

    private SessionPool sessionPool;

    private TransactionManager transactionManager;

    private String name = null;

    private boolean stopped;

    private boolean closed;

    private boolean clientIdSetSinceReopen = false;

    private PooledXAConnectionFactory pooledConnectionFactory;

    private int referenceCount;
    private long lastUsed = System.currentTimeMillis();

    public PooledXAConnection(
            final PooledXAConnectionFactory pooledConnectionFactory,
            final TransactionManager transactionManager,
            final XAConnection connection) {
        this(pooledConnectionFactory, transactionManager, new ConnectionInfo(
                connection), new SessionPool(connection));
    }

    public PooledXAConnection(
            final PooledXAConnectionFactory pooledConnectionFactory,
            final TransactionManager transactionManager,
            final ConnectionInfo connectionInfo, final SessionPool sessionPool) {
        this.pooledConnectionFactory = pooledConnectionFactory;
        this.transactionManager = transactionManager;
        this.connectionInfo = connectionInfo;
        this.sessionPool = sessionPool;
        this.closed = false;
    }

    protected void setName(String newName) {
        this.name = newName;
    }

    synchronized public void incrementReferenceCount() {
        referenceCount++;
    }

    synchronized public void decrementReferenceCount() {
        referenceCount--;
        lastUsed = System.currentTimeMillis();
        if( referenceCount == 0 ) {
            expiredCheck();
        }
    }

    synchronized public boolean expiredCheck() {
        /*
        if (connection == null) {
            return true;
        }
        long t = System.currentTimeMillis();
        if( hasFailed || idleTimeout> 0 && t > lastUsed+idleTimeout ) {
            if( referenceCount == 0 ) {
                close();
            }
            return true;
        }
        */
        return false;
    }

    /**
     * Factory method to create a new instance.
     */
    public PooledXAConnection newInstance() {

        PooledXAConnection newInstance =
                new PooledXAConnection(this.pooledConnectionFactory,
                        this.transactionManager, this.connectionInfo, this.sessionPool);
        if (name != null) {
            newInstance.setName(name);
        }
        return newInstance;
    }

    public void close() {
        this.closed = true;
    }

    public void start() throws JMSException {
        // TODO should we start connections first before pooling them?
        getConnection().start();
    }

    public void stop() throws JMSException {
        this.stopped = true;
    }

    public ConnectionConsumer createConnectionConsumer(
            final Destination destination, final String selector,
            final ServerSessionPool serverSessionPool, final int maxMessages)
            throws JMSException {
        return getConnection().createConnectionConsumer(destination, selector,
                serverSessionPool, maxMessages);
    }

    public ConnectionConsumer createConnectionConsumer(Topic topic, String s,
                                                       ServerSessionPool serverSessionPool, int maxMessages)
            throws JMSException {
        return getConnection().createConnectionConsumer(topic, s,
                serverSessionPool, maxMessages);
    }

    public ConnectionConsumer createDurableConnectionConsumer(
            final Topic topic, final String selector, final String s1,
            final ServerSessionPool serverSessionPool, final int i)
            throws JMSException {
        return getConnection().createDurableConnectionConsumer(topic, selector,
                s1, serverSessionPool, i);
    }

    public String getClientID() throws JMSException {
        return getConnection().getClientID();
    }

    public ExceptionListener getExceptionListener() throws JMSException {
        return getConnection().getExceptionListener();
    }

    public ConnectionMetaData getMetaData() throws JMSException {
        return getConnection().getMetaData();
    }

    public void setExceptionListener(ExceptionListener exceptionListener)
            throws JMSException {
        getConnection().setExceptionListener(exceptionListener);
    }

    public void setClientID(String clientID) throws JMSException {
        if (this.clientIdSetSinceReopen) {
            throw new JMSException(
                    "ClientID is already set on this connection.");
        }

        synchronized (this.connectionInfo) {
            if (this.connectionInfo.isActualClientIdSet()) {
                if (this.connectionInfo.getActualClientIdBase() == null ? clientID != null
                        : !this.connectionInfo.getActualClientIdBase().equals(
                        clientID)) {
                    throw new JMSException(
                            "A pooled Connection must only ever have its client ID set to the same value for the duration of the pooled ConnectionFactory.  It looks like code has set a client ID, returned the connection to the pool, and then later obtained the connection from the pool and set a different client ID.");
                }
            } else {
                final String generatedId = getPooledConnectionFactory()
                        .generateClientID(clientID);
                getConnection().setClientID(generatedId);
                this.connectionInfo.setActualClientIdBase(clientID);
                this.connectionInfo.setActualClientIdSet(true);
            }
        }

        this.clientIdSetSinceReopen = true;
    }

    public ConnectionConsumer createConnectionConsumer(final Queue queue,
                                                       final String selector, final ServerSessionPool serverSessionPool,
                                                       final int maxMessages) throws JMSException {
        return getConnection().createConnectionConsumer(queue, selector,
                serverSessionPool, maxMessages);
    }

    public XASession createXASession() throws JMSException {
        try {
            if (this.transactionManager.getStatus() != Status.STATUS_NO_TRANSACTION) {
                final PooledSession newSession = this.sessionPool.borrowSession();
                newSession.setIgnoreClose(true);
                if (name != null) {
                    newSession.setName(name);
                }
                this.transactionManager.getTransaction().registerSynchronization(new Synchronization(newSession));
                incrementReferenceCount();
                this.transactionManager.getTransaction().enlistResource(newSession.getXAResource());
                return newSession;
            } else {
                return this.sessionPool.borrowSession();
            }
        } catch (SystemException e) {
            throw (JMSException) new JMSException("System exception").initCause(e);
        } catch (RollbackException e) {
            throw (JMSException) new JMSException("Rollback exception").initCause(e);
        }
    }

    // Session factory methods
    //-------------------------------------------------------------------------
    public QueueSession createQueueSession(boolean transacted, int ackMode)
            throws JMSException {
        return (QueueSession) createSession(transacted, ackMode);
    }

    public TopicSession createTopicSession(boolean transacted, int ackMode)
            throws JMSException {
        return (TopicSession) createSession(transacted, ackMode);
    }

    public Session createSession(boolean transacted, int ackMode)
            throws JMSException {
        return createXASession();
    }

    // Implementation methods
    //-------------------------------------------------------------------------
    protected XAConnection getConnection() throws JMSException {
        if (this.stopped || this.closed) {
            throw new JMSException("Already closed");
        }
        return this.connectionInfo.getConnection();
    }

    public PooledXAConnectionFactory getPooledConnectionFactory() {
        return this.pooledConnectionFactory;
    }

    private class Synchronization implements javax.transaction.Synchronization {
        private final PooledSession session;

        private Synchronization(PooledSession session) {
            this.session = session;
        }

        public void beforeCompletion() {
        }

        public void afterCompletion(int status) {
            try {
                // This will return session to the pool.
                session.setIgnoreClose(false);
                session.close();
                decrementReferenceCount();
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ConnectionInfo {
        private XAConnection connection;

        private boolean actualClientIdSet;

        private String actualClientIdBase;

        public ConnectionInfo(final XAConnection connection) {
            this.connection = connection;
            this.actualClientIdSet = false;
            this.actualClientIdBase = null;
        }

        public XAConnection getConnection() {
            return connection;
        }

        public void setConnection(final XAConnection connection) {
            this.connection = connection;
        }

        public synchronized boolean isActualClientIdSet() {
            return actualClientIdSet;
        }

        public synchronized void setActualClientIdSet(
                final boolean actualClientIdSet) {
            this.actualClientIdSet = actualClientIdSet;
        }

        public synchronized String getActualClientIdBase() {
            return actualClientIdBase;
        }

        public synchronized void setActualClientIdBase(
                final String actualClientIdBase) {
            this.actualClientIdBase = actualClientIdBase;
        }
    }
}
