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

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageProducer;

/**
 * A pooled {@link javax.jms.MessageProducer}
 */
public class PooledProducer implements MessageProducer {

    private MessageProducer messageProducer;
    private Destination destination;
    private int deliveryMode;
    private boolean disableMessageID;
    private boolean disableMessageTimestamp;
    private int priority;
    private long timeToLive;

    public PooledProducer(final MessageProducer messageProducer,
                          final Destination destination) throws JMSException {
        this.messageProducer = messageProducer;
        this.destination = destination;

        this.deliveryMode = messageProducer.getDeliveryMode();
        this.disableMessageID = messageProducer.getDisableMessageID();
        this.disableMessageTimestamp = messageProducer.getDisableMessageTimestamp();
        this.priority = messageProducer.getPriority();
        this.timeToLive = messageProducer.getTimeToLive();
    }

    public void close() throws JMSException {
    }

    public void send(final Destination destination, final Message message)
            throws JMSException {
        send(destination, message, getDeliveryMode(), getPriority(), getTimeToLive());
    }

    public void send(final Message message) throws JMSException {
        send(this.destination, message, getDeliveryMode(), getPriority(), getTimeToLive());
    }

    public void send(final Message message, final int deliveryMode, final int priority,
                     final long timeToLive) throws JMSException {
        send(this.destination, message, deliveryMode, priority, timeToLive);
    }

    public void send(Destination destination, final Message message, final int deliveryMode,
                     final int priority, final long timeToLive) throws JMSException {
        if (destination == null) {
            destination = this.destination;
        }
        MessageProducer messageProducer = getMessageProducer();

        // just in case let only one thread send at once
        synchronized (messageProducer) {
            messageProducer.send(destination, message, deliveryMode, priority, timeToLive);
        }
    }

    public Destination getDestination() {
        return this.destination;
    }

    public int getDeliveryMode() {
        return this.deliveryMode;
    }

    public void setDeliveryMode(final int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    public boolean getDisableMessageID() {
        return this.disableMessageID;
    }

    public void setDisableMessageID(final boolean disableMessageID) {
        this.disableMessageID = disableMessageID;
    }

    public boolean getDisableMessageTimestamp() {
        return this.disableMessageTimestamp;
    }

    public void setDisableMessageTimestamp(final boolean disableMessageTimestamp) {
        this.disableMessageTimestamp = disableMessageTimestamp;
    }

    public int getPriority() {
        return this.priority;
    }

    public void setPriority(final int priority) {
        this.priority = priority;
    }

    public long getTimeToLive() {
        return this.timeToLive;
    }

    public void setTimeToLive(final long timeToLive) {
        this.timeToLive = timeToLive;
    }

    // Implementation methods
    //-------------------------------------------------------------------------
    protected MessageProducer getMessageProducer() {
        return this.messageProducer;
    }
}
