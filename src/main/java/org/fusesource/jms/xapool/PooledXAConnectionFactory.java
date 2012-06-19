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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.transaction.TransactionManager;

public class PooledXAConnectionFactory implements ConnectionFactory {

    private XAConnectionFactory connectionFactory;

    private TransactionManager transactionManager;

    private Map cache = new ConcurrentHashMap();

    private Map pooledClientIds = new ConcurrentHashMap();

    // resource name to be used with XA tx recovery log
    private String name = null;


    public PooledXAConnectionFactory() {
    }

    public PooledXAConnectionFactory(
            final XAConnectionFactory connectionFactory,
            final TransactionManager transactionManager) {
        this.connectionFactory = connectionFactory;
        this.transactionManager = transactionManager;
    }

    public String getName() {
        return name;
    }

    public void setName(String newName) {
        this.name = newName;
    }

    public TransactionManager getTransactionManager() {
        return this.transactionManager;
    }

    public void setTransactionManager(final TransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public XAConnectionFactory getConnectionFactory() {
        return this.connectionFactory;
    }

    public void setConnectionFactory(final XAConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public Connection createConnection() throws JMSException {
        return createConnection(null, null);
    }

    public synchronized Connection createConnection(final String userName, final String password) throws JMSException {
        final ConnectionKey key = new ConnectionKey(userName, password);
        PooledXAConnection connection = (PooledXAConnection) this.cache.get(key);
        if (connection == null) {
            final XAConnection delegate = createConnection(key);
            connection = new PooledXAConnection(this, getTransactionManager(), delegate);

            if (name != null) {
                connection.setName(name);
            }
            this.cache.put(key, connection);
        }
        return connection.newInstance();
    }

    protected XAConnection createConnection(ConnectionKey key) throws JMSException {
        if (key.getUserName() == null && key.getPassword() == null) {
            return this.connectionFactory.createXAConnection();
        } else {
            return this.connectionFactory.createXAConnection(key.getUserName(), key.getPassword());
        }
    }

    public String generateClientID(final String requestedClientID) {
        if (requestedClientID == null) {
            return null;
        }

        final int num;

        synchronized (this) {
            final Integer lastCount = (Integer) this.pooledClientIds
                    .get(requestedClientID);
            if (lastCount == null) {
                num = 1;
            } else {
                num = lastCount.intValue() + 1;
            }
            this.pooledClientIds.put(requestedClientID, new Integer(num));
        }

        return requestedClientID + "-" + num;
    }

    public void start() throws JMSException {
    }

    public void stop() throws JMSException {
    }
}
