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

import javax.jms.JMSException;
import javax.jms.XAConnection;
import javax.jms.XAConnectionFactory;
import javax.jms.XASession;
import javax.transaction.SystemException;

import org.apache.geronimo.transaction.manager.NamedXAResource;
import org.apache.geronimo.transaction.manager.NamedXAResourceFactory;
import org.apache.geronimo.transaction.manager.RecoverableTransactionManager;
import org.apache.geronimo.transaction.manager.WrapperNamedXAResource;

public class ResourceManager implements NamedXAResourceFactory {

    private RecoverableTransactionManager transactionManager;
    private XAConnectionFactory connectionFactory;
    private String name;

    public void recover() throws JMSException {
        transactionManager.registerNamedXAResourceFactory(this);
    }

    public NamedXAResource getNamedXAResource() throws SystemException {
        try {
            XAConnection connection = connectionFactory.createXAConnection();
            XASession session = connection.createXASession();
            RecoveryWrapperNamedXAResource namedXaResource = new RecoveryWrapperNamedXAResource(connection,  session);
            return namedXaResource;
        } catch (JMSException e) {
            throw (SystemException) new SystemException("Error creating JMS connection for recovery").initCause(e);
        }
    }

    public void returnNamedXAResource(NamedXAResource namedXAResource) {
        ((RecoveryWrapperNamedXAResource) namedXAResource).close();
    }

    public RecoverableTransactionManager getTransactionManager() {
        return transactionManager;
    }

    public void setTransactionManager(RecoverableTransactionManager transactionManager) {
        this.transactionManager = transactionManager;
    }

    public XAConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public void setConnectionFactory(XAConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    protected class RecoveryWrapperNamedXAResource extends WrapperNamedXAResource {
        private XAConnection connection;
        private XASession session;

        public RecoveryWrapperNamedXAResource(XAConnection connection, XASession session) throws JMSException {
            super(session.getXAResource(), name);
            this.connection = connection;
            this.session = session;
        }

        public void close() {
            try {
                session.close();
            } catch (JMSException e) {
                // Ignore
            }
            try {
                connection.close();
            } catch (JMSException e) {
                // Ignore
            }
        }
    }

}
