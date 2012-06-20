Generic XA-aware JMS Connection Pool
====================================

This library provides a generic JMS connection pool with 3 different falvors:
  * a simple [PooledConnectionFactory](https://github.com/fusesource/jmspool/blob/master/src/main/java/org/fusesource/jms/pool/PooledConnectionFactory.java)
  * an XA-aware [XAPooledConnectionFactory](https://github.com/fusesource/jmspool/blob/master/src/main/java/org/fusesource/jms/pool/XaPooledConnectionFactory.java)
  * a [JCAPooledConnectionFactory](https://github.com/fusesource/jmspool/blob/master/src/main/java/org/fusesource/jms/pool/JcaPooledConnectionFactory.java) dedicated to leverage the Geronimo/Aries transaction manager

# Configuration

## Base configuration

The PooledConnectionFactory has the following mandatory parameter:
  * connectionFactory: the underlying JMS ConnectionFactory 
and the following optional parameters:
  * maximumActive: maximum number of sessions for a given connection (used when no poolFactory is given, default to 500)
  * maxConnections: the maximum number of connections to use (default to 1)
  * idleTimeout: allow connections to expire after the given idle timeout (default to 30s)
  * blockIfSessionPoolIsFull: blocks if the pool is full when retrieving a session (default to true)
  * expiryTimeout: allow connections to expire, irrespective of load or idle time (default to 0, meaning no expiry)
  * poolFactory: the underlying ObjectPoolFactory to customize the pool itself (based on Apache Commons-Pool)

## XA connection pool

In addition to the above settings, the XAPooledConnectionFactory has the following mandatory parameter:
  * transactionManager: the JTA TransactionManager to use
and the following optional parameters:
  * xaConnectionFactory: an XAConnectionFactory which can be set if the XAConnectionFactory does not implement ConnectionFactory

## JCA connection pool

To work with the Geronimo Transaction Manager and allow proper recovery, a specific connection pool should be used.

In addition to the above settings, the JCAPooledConnectionFactory has the following mandatory paramter:
  * name: the name of the resource manager (used by the JTA transaction manager to detect if 2 phase commit must be used)

The resource manager name has to uniquely identify the broker.

To be able to start the recovery process, the [GenericResourceManager](https://github.com/fusesource/jmspool/blob/master/src/main/java/org/fusesource/jms/pool/GenericResourceManager.java) must be configured.

# Examples

## Simple pool using blueprint

       <bean id="internalConnectionFactory" class="org.apache.activemq.ActiveMQXAConnectionFactory">
           <argument value="tcp://localhost:61616" />
       </bean>
	
       <bean id="connectionFactory" class="org.fusesource.jms.pool.JcaPooledConnectionFactory" 
               init-method="start" destroy-method="stop">
           <property name="connectionFactory" ref="internalConnectionFactory"/>
           <property name="name" value="activemq" />
       </bean>


## JCA pool

       <reference id="transactionManager" interface="org.apache.geronimo.transaction.manager.RecoverableTransactionManager" 
               availability="mandatory" />

       <bean id="platformTransactionManager" class="org.springframework.transaction.jta.JtaTransactionManager" 
               init-method="afterPropertiesSet">
           <property name="transactionManager" ref="transactionManager"/>
           <property name="autodetectUserTransaction" value="false"/>
       </bean>

       <bean id="internalConnectionFactory" class="org.apache.activemq.ActiveMQXAConnectionFactory">
           <argument value="tcp://localhost:61616" />
       </bean>
	
       <bean id="connectionFactory" class="org.fusesource.jms.pool.JcaPooledConnectionFactory"
               init-method="start" destroy-method="stop">
           <property name="connectionFactory" ref="internalConnectionFactory"/>
           <property name="transactionManager" ref="transactionManager"/>
           <property name="name" value="activemq" />
       </bean>
	
       <bean id="resourceManager" class="org.fusesource.jms.pool.GenericResourceManager" init-method="recoverResource">
           <property name="connectionFactory" ref="internalConnectionFactory"/>
           <property name="transactionManager" ref="transactionManager"/>
           <property name="resourceName" value="activemq" />
       </bean>
    


