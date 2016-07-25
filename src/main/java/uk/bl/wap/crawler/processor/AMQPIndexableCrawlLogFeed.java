/**
 * 
 */
package uk.bl.wap.crawler.processor;

import java.io.UnsupportedEncodingException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.apache.http.HttpHeaders;
import org.archive.modules.CrawlURI;
import org.archive.modules.postprocessor.AMQPCrawlLogFeed;
import org.archive.modules.postprocessor.CrawlLogJsonBuilder;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.json.JSONObject;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.AMQP.BasicProperties;

/**
 * 
 * Variation of AMQPCrawlLogFeed that includes the redirect url and makes the
 * messages persistent.
 * 
 * Optionally use the Celery message format so that framework can process the
 * tasks easily.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class AMQPIndexableCrawlLogFeed extends AMQPCrawlLogFeed {

    private final static Logger LOGGER = Logger
            .getLogger(AMQPIndexableCrawlLogFeed.class.getName());

    private boolean celeryMessageFormat = false;

    private String targetCeleryTask = "crawl.tasks.index_uri";

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
            try {
                // Cope with absolute or relative URLs etc.
                UURI dest = UURIFactory.getInstance(curi.getUURI(), location);
                // Store it:
                jo.put("redirecturl", dest.toString());
            } catch (URIException e1) {
                LOGGER.log(Level.SEVERE,
                        "Could not parse redirect Location: " + location);
            }
        }
        // Re-wrap for Celery if required:
        if (this.celeryMessageFormat) {
            jo = this.wrapForCelery(jo);
        }
        // Encode and return:
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

    /**
     * 
     * Re-wrap the JSON message in suitable package for sending to a Python
     * Celery task.
     * 
     * @see http://docs.celeryproject.org/en/latest/internals/protocol.html
     * 
     * @param jo
     * @return
     * 
     */
    protected JSONObject wrapForCelery(JSONObject payload) {
        //
        // {
        // "id": "e7224d1c-3d15-4478-98e9-2ce39f0f9c53",
        // "task": "crawl.tasks.verify_sip",
        // "args": [],
        // "kwargs": {}
        // }
        //
        JSONObject jo = new JSONObject();
        jo.put("id", UUID.randomUUID());
        jo.put("task", this.targetCeleryTask);
        String[] args = {};
        jo.put("args", args);
        jo.put("kwargs", payload);
        return jo;
    }

    /**
     * @return use the celeryMessageFormat?
     */
    public boolean isCeleryMessageFormat() {
        return celeryMessageFormat;
    }

    /**
     * @param celeryMessageFormat
     *            whether to use the Celery message format
     */
    public void setCeleryMessageFormat(boolean celeryMessageFormat) {
        this.celeryMessageFormat = celeryMessageFormat;
    }

    /**
     * @return the targetCeleryTask
     */
    public String getTargetCeleryTask() {
        return targetCeleryTask;
    }

    /**
     * @param targetCeleryTask
     *            the targetCeleryTask to set
     */
    public void setTargetCeleryTask(String targetCeleryTask) {
        this.targetCeleryTask = targetCeleryTask;
    }

}
