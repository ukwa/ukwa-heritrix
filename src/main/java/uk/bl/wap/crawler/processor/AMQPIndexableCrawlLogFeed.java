/**
 * 
 */
package uk.bl.wap.crawler.processor;

import java.io.UnsupportedEncodingException;

import org.apache.http.HttpHeaders;
import org.archive.modules.CrawlURI;
import org.archive.modules.postprocessor.AMQPCrawlLogFeed;
import org.archive.modules.postprocessor.CrawlLogJsonBuilder;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class AMQPIndexableCrawlLogFeed extends AMQPCrawlLogFeed {

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.modules.postprocessor.AMQPCrawlLogFeed#buildMessage(org.
     * archive.modules.CrawlURI)
     */
    @Override
    protected byte[] buildMessage(CrawlURI curi) {
        // Build standard object
        JSONObject jo = CrawlLogJsonBuilder.buildJson(curi, getExtraFields(),
                getServerCache());
        // Patch on the location, so we can index redirects:
        String location = curi.getHttpResponseHeader(HttpHeaders.LOCATION);
        if (location != null) {
            jo.put("redirecturl", location);
        }
        try {
            return jo.toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 
     * Ensure messages are persistent, using delivery mode 2.
     * 
     */
    protected BasicProperties props = new AMQP.BasicProperties.Builder()
            .contentType("application/json").deliveryMode(2).build();

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.archive.modules.postprocessor.AMQPCrawlLogFeed#amqpMessageProperties(
     * )
     */
    @Override
    protected BasicProperties amqpMessageProperties() {
        return props;
    }

}
