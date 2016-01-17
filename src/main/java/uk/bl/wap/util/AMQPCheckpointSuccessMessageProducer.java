/**
 * 
 */
package uk.bl.wap.util;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.checkpointing.Checkpoint;
import org.archive.crawler.event.CrawlStateEvent;
import org.archive.crawler.framework.CheckpointSuccessEvent;
import org.archive.crawler.framework.CrawlController.StopCompleteEvent;
import org.archive.modules.AMQPProducer;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * 
 * This simple ApplicationListener looks out for CheckpointSuccess events and
 * sends the detail on via AMQP so the checkpointed files can be dealt with.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class AMQPCheckpointSuccessMessageProducer
        implements ApplicationListener<ApplicationEvent> {
    private final static Logger LOGGER = Logger
            .getLogger(AMQPCheckpointSuccessMessageProducer.class.getName());

    protected String amqpUri = "amqp://guest:guest@localhost:5672/%2f";

    public String getAmqpUri() {
        return this.amqpUri;
    }

    public void setAmqpUri(String uri) {
        this.amqpUri = uri;
    }

    protected String exchange;

    public String getExchange() {
        return exchange;
    }

    public void setExchange(String exchange) {
        this.exchange = exchange;
    }

    protected String routingKey;

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    transient protected AMQPProducer amqpProducer;

    protected AMQPProducer amqpProducer() {
        if (amqpProducer == null) {
            amqpProducer = new AMQPProducer(getAmqpUri(), getExchange(),
                    getRoutingKey());
        }
        return amqpProducer;
    }

    /**
     * Log note of all ApplicationEvents.
     * 
     * @see org.springframework.context.ApplicationListener#onApplicationEvent(org.springframework.context.ApplicationEvent)
     */
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof CrawlStateEvent) {
            LOGGER.log(Level.INFO,
                    "Crawl state is: " + ((CrawlStateEvent) event).getState());
        }

        if (event instanceof StopCompleteEvent) {
            LOGGER.log(Level.INFO, "Stop Complete Event.");
            if (amqpProducer != null) {
                amqpProducer.stop();
            }
        }

        // We want to publish checkpoint events.
        if (event instanceof CheckpointSuccessEvent) {
            Checkpoint cp = ((CheckpointSuccessEvent) event).getCheckpoint();

            String eventName = cp.getName();
            LOGGER.log(Level.INFO, "CHECKPOINTED " + eventName);

            String path = cp.getCheckpointDir().getPath();
            LOGGER.log(Level.INFO, "CHECKPOINTED IN " + path);
            sendApplicationEventMessage(path.getBytes());
        }
    }

    protected BasicProperties props = new AMQP.BasicProperties.Builder()
            .contentType("application/json").deliveryMode(2).build();

    protected void sendApplicationEventMessage(byte[] message) {
        try {
            amqpProducer().publishMessage(message, props);
            LOGGER.info("Send AMQP message for event.");
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "failed to send message to amqp event.",
                    e);
        }
    }

}
