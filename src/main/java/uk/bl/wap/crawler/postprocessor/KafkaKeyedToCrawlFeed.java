/**
 * 
 */
package uk.bl.wap.crawler.postprocessor;

import static org.archive.modules.CoreAttributeConstants.A_HERITABLE_KEYS;

import java.io.UnsupportedEncodingException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.URIException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideRule;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * 
 * Sub-class that builds a CrawlRequest rather than the usual Crawl Log output.
 * 
 * Based on: @see AMQPPublishProcessor and @see KafkaKeyedCrawlLogFeed
 * and @KafkaUrlReceiver
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class KafkaKeyedToCrawlFeed extends KafkaKeyedCrawlLogFeed {
    
    protected DecideRule scope;

    public DecideRule getScope() {
        return this.scope;
    }

    @Autowired
    public void setScope(DecideRule scope) {
        this.scope = scope;
    }

    /**
     * Constructs the json to send.
     * 
     * @return the message to send.
     * @see CrawlURI#inheritFrom(CrawlURI)
     */
    protected JSONObject buildJsonMessage(CrawlURI source, CrawlURI curi) {

        // Set up object concerning the new URI:
        JSONObject message = new JSONObject().put("url", curi.toString());
        message.put("isSeed", curi.isSeed());
        message.put("forceFetch", curi.forceFetch());
        message.put("hop", curi.getLastHop());
        message.put("method", "GET");
        message.put("headers", curi.getData().get("customHttpRequestHeaders"));

        /*
         * if (getExtraFields() != null) { for (String k :
         * getExtraFields().keySet()) { message.put(k, getExtraFields().get(k));
         * } }
         */

        // Also store metadata about the parent URI:
        message.put("parentUrl", source.getURI());
        HashMap<String, Object> metadata = new HashMap<String, Object>();
        metadata.put("pathFromSeed", source.getPathFromSeed());

        @SuppressWarnings("unchecked")
        Set<String> heritableKeys = (Set<String>) source.getData()
                .get(A_HERITABLE_KEYS);
        HashMap<String, Object> heritableData = new HashMap<String, Object>();
        if (heritableKeys != null) {
            for (String key : heritableKeys) {
                heritableData.put(key, source.getData().get(key));
            }
        }
        metadata.put("heritableData", heritableData);
        message.put("parentUrlMetadata", metadata);

        return message;
    }

    protected byte[] buildMessage(CrawlURI source, CrawlURI curi) {
        try {
            return buildJsonMessage(source, curi).toString().getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * This cache is used to avoid too much duplication of discovered URLs.
     * However, note that this will interfere with re-crawling dynamics over
     * short times.
     */
    private Cache<String, Boolean> recentlySentCache = CacheBuilder
            .newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).softValues()
            .maximumSize(2000).build();

    private void sendToKafka(String topic, CrawlURI curi, CrawlURI candidate) {
        // Check if this URL has been sent recently:
        Boolean recentlySent = recentlySentCache
                .getIfPresent(candidate.getURI());
        if (recentlySent == null) {
            // Make a suitable key:
            String key = this.getKeyForCrawlURI(candidate);
            // Send
            logger.finer("Sending a message wrapping URI: " + candidate
                    + " to topic " + topic);
            byte[] message = buildMessage(curi, candidate);
            ProducerRecord<String, byte[]> producerRecord = new ProducerRecord<String, byte[]>(
                    topic, key, message);
            kafkaProducer().send(producerRecord, stats);
            recentlySentCache.put(candidate.getURI(), true);
        }
    }

    @Override
    protected String getKeyForCrawlURI(CrawlURI curi) {
        try {
            return curi.getUURI().getHost();
        } catch (URIException ue) {
            return "malformed-uri";
        }
    }

    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        // Process all outlinks:
        Collection<CrawlURI> outLinks = curi.getOutLinks();
        Collection<CrawlURI> toRemove = new LinkedHashSet<CrawlURI>();
        for (CrawlURI candidate : outLinks) {
            // Route most via Kafka:
            if (!candidate.isPrerequisite()) {
                if (this.getScope().accepts(candidate)) {
                    sendToKafka(getTopic(), curi, candidate);
                } else {
                    // TODO Log discarded URLs for analysis:
                    // sendToKafka("uris-outofscope", curi, candidate);
                }
                // Record this diverted URL so it will not be queued
                // directly:
                toRemove.add(candidate);
            }
        }
        // And remove re-routed candidates from the candidates list:
        for (CrawlURI candidate : toRemove) {
            outLinks.remove(candidate);
        }
    }
}