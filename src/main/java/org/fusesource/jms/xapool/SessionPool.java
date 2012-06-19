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
import javax.jms.XASession;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.PoolableObjectFactory;
import org.apache.commons.pool.impl.GenericObjectPool;

public class SessionPool implements PoolableObjectFactory {
    private static final Log log = LogFactory.getLog(SessionPool.class);

    private XAConnection connection;
    private ObjectPool sessionPool;

    public SessionPool(final XAConnection connection) {
        this(connection, new GenericObjectPool(null, -1));
    }

    public SessionPool(final XAConnection connection,
                       final ObjectPool sessionPool) {
        this.connection = connection;
        this.sessionPool = sessionPool;
        sessionPool.setFactory(this);
    }

    public PooledSession borrowSession() throws JMSException {
        try {
            if (log.isDebugEnabled()) log.debug("---->>>>> BORROWING JMS SESSION FROM POOL...");
            Object object = sessionPool.borrowObject();
            if (log.isDebugEnabled()) log.debug("---->>>>> BORROWED SESSION: " + object);
            return (PooledSession) object;
        } catch (JMSException e) {
            throw e;
        } catch (Exception e) {
            final JMSException jmsException = new JMSException("Unhandled exception");
            jmsException.initCause(e);
            throw jmsException;
        }
    }


    // PoolableObjectFactory methods
    //-------------------------------------------------------------------------
    public Object makeObject() throws Exception {
        if (log.isDebugEnabled()) log.debug("---->>>>> CREATING NEW SESSION TO SATISFY REQUEST!!");
        return new PooledSession(createSession(), sessionPool);
    }

    public void destroyObject(Object o) throws Exception {
        if (log.isDebugEnabled()) log.debug("---->>>>> DESTROYING SESSION AND PERMANENTLY REMOVING FROM POOL: " + o);
        PooledSession session = (PooledSession) o;
        session.getActualSession().close();
    }

    public boolean validateObject(Object o) {
        return true;
    }

    public void activateObject(Object o) throws Exception {
    }

    public void passivateObject(Object o) throws Exception {
        if (log.isDebugEnabled()) log.debug("---->>>>> SESSION HAS BEEN RETURNED TO POOL: " + o);
    }

    // Implemention methods
    //-------------------------------------------------------------------------
    protected XAConnection getConnection() throws JMSException {
        if (this.connection == null) {
            throw new JMSException("Already closed");
        }
        return connection;
    }

    protected XASession createSession() throws JMSException {
        return getConnection().createXASession();
    }


}
