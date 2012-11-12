/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.fusesource.jms.pool;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.commons.pool.ObjectPoolFactory;

/**
 * Holds a real JMS connection along with the session pools associated with it.
 *
 *
 */
public class ConnectionPool {

    private Connection connection;
    private ConcurrentHashMap<SessionKey, SessionPool> cache;
    private List<PooledSession> loanedSessions = new CopyOnWriteArrayList<PooledSession>();
    private AtomicBoolean started = new AtomicBoolean(false);
    private int referenceCount;
    private ObjectPoolFactory poolFactory;
    private long lastUsed = System.currentTimeMillis();
    private long firstUsed = lastUsed;
    private boolean hasFailed;
    private boolean hasExpired;
    private int idleTimeout = 30 * 1000;
    private long expiryTimeout = 0l;

    public ConnectionPool(Connection connection, ObjectPoolFactory poolFactory) throws JMSException {
        this(connection, new ConcurrentHashMap<SessionKey, SessionPool>(), poolFactory);
        /*
        TODO: activemq specific
        // Add a transport Listener so that we can notice if this connection
        // should be expired due to a connection failure.
        connection.addTransportListener(new TransportListener() {
            public void onCommand(Object command) {
            }

            public void onException(IOException error) {
                synchronized (ConnectionPool.this) {
                    hasFailed = true;
                }
            }

            public void transportInterupted() {
            }

            public void transportResumed() {
            }
        });

        // make sure that we set the hasFailed flag, in case the transport already failed
        // prior to the addition of our new TransportListener
        if(connection.isTransportFailed()) {
            hasFailed = true;
        }
        */
        connection.setExceptionListener(new ExceptionListener() {
            public void onException(JMSException exception) {
                synchronized (ConnectionPool.this) {
                    hasFailed = true;
                }
            }
        });
    }

    public ConnectionPool(Connection connection, ConcurrentHashMap<SessionKey, SessionPool> cache, ObjectPoolFactory poolFactory) {
        this.connection = connection;
        this.cache = cache;
        this.poolFactory = poolFactory;
    }

    public void start() throws JMSException {
        if (started.compareAndSet(false, true)) {
            try {
                connection.start();
            } catch (JMSException e) {
                started.set(false);
                throw(e);
            }
        }
    }

    public synchronized javax.jms.Connection getConnection() {
        return connection;
    }

    public Session createSession(boolean transacted, int ackMode) throws JMSException {
        SessionKey key = new SessionKey(transacted, ackMode);
        SessionPool pool = null;
        pool = cache.get(key);
        if (pool == null) {
            SessionPool newPool = createSessionPool(key);
            SessionPool prevPool = cache.putIfAbsent(key, newPool);
            if (prevPool != null && prevPool != newPool) {
                // newPool was not the first one to be associated with this
                // key... close created session pool
                try {
                    newPool.close();
                } catch (Exception e) {
                    throw new JMSException(e.getMessage());
                }
            }
            pool = cache.get(key); // this will return a non-null value...
        }
        PooledSession session = pool.borrowSession();
        this.loanedSessions.add(session);
        return session;
    }
    
    
    public Session createXaSession(boolean transacted, int ackMode) throws JMSException {
        SessionKey key = new SessionKey(transacted, ackMode);
        SessionPool pool = null;
        pool = cache.get(key);
        if (pool == null) {
            SessionPool newPool = createSessionPool(key);
            SessionPool prevPool = cache.putIfAbsent(key, newPool);
            if (prevPool != null && prevPool != newPool) {
                // newPool was not the first one to be associated with this
                // key... close created session pool
                try {
                    newPool.close();
                } catch (Exception e) {
                    throw new JMSException(e.getMessage());
                }
            }
            pool = cache.get(key); // this will return a non-null value...
        }
        PooledSession session = pool.borrowSession();
        this.loanedSessions.add(session);
        return session;
    }
    

    public synchronized void close() {
        if (connection != null) {
            try {
                Iterator<SessionPool> i = cache.values().iterator();
                while (i.hasNext()) {
                    SessionPool pool = i.next();
                    i.remove();
                    try {
                        pool.close();
                    } catch (Exception e) {
                    }
                }
            } finally {
                try {
                    connection.close();
                } catch (Exception e) {
                } finally {
                    connection = null;
                }
            }
        }
    }

    public synchronized void incrementReferenceCount() {
        referenceCount++;
        lastUsed = System.currentTimeMillis();
    }

    public synchronized void decrementReferenceCount() {
        referenceCount--;
        lastUsed = System.currentTimeMillis();
        if (referenceCount == 0) {
            expiredCheck();

            for (PooledSession session : this.loanedSessions) {
                try {
                    session.close();
                } catch (Exception e) {
                }
            }
            this.loanedSessions.clear();

            // only clean up temp destinations when all users
            // of this connection have called close
            if (getConnection() != null) {
                /*
                TODO: activemq specific
                getConnection().cleanUpTempDestinations();
                */
            }
        }
    }

    /**
     * @return true if this connection has expired.
     */
    public synchronized boolean expiredCheck() {
        if (connection == null) {
            return true;
        }
        if (hasExpired) {
            if (referenceCount == 0) {
                close();
            }
            return true;
        }
        if (hasFailed
                || (idleTimeout > 0 && System.currentTimeMillis() > lastUsed + idleTimeout)
                || expiryTimeout > 0 && System.currentTimeMillis() > firstUsed + expiryTimeout) {
            hasExpired = true;
            if (referenceCount == 0) {
                close();
            }
            return true;
        }
        return false;
    }

    public int getIdleTimeout() {
        return idleTimeout;
    }

    public void setIdleTimeout(int idleTimeout) {
        this.idleTimeout = idleTimeout;
    }

    protected SessionPool createSessionPool(SessionKey key) {
        return new SessionPool(this, key, poolFactory.createPool());
    }

    public void setExpiryTimeout(long expiryTimeout) {
        this.expiryTimeout  = expiryTimeout;
    }

    public long getExpiryTimeout() {
        return expiryTimeout;
    }

    void onSessionReturned(PooledSession session) {
        this.loanedSessions.remove(session);
    }

    void onSessionInvalidated(PooledSession session) {
        this.loanedSessions.remove(session);
    }
}
