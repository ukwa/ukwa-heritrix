/**
 * 
 */
package uk.bl.wap.crawler.postprocessor;

import static org.archive.modules.CoreAttributeConstants.A_HERITABLE_KEYS;

import java.io.UnsupportedEncodingException;
import java.net.IDN;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.httpclient.URIException;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.archive.crawler.spring.SheetOverlaysManager;
import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideRule;
import org.archive.spring.KeyedProperties;
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
    
    private boolean emitInScopeOnly = false;

    public boolean isEmitInScopeOnly() {
        return emitInScopeOnly;
    }

    public void setEmitInScopeOnly(boolean emitInScopeOnly) {
        this.emitInScopeOnly = emitInScopeOnly;
    }

    private boolean emitOutlinks = true;

    public boolean isEmitOutlinks() {
        return emitOutlinks;
    }

    public void setEmitOutlinks(boolean emitOutlinks) {
        this.emitOutlinks = emitOutlinks;
    }

    protected DecideRule scope;

    public DecideRule getScope() {
        return this.scope;
    }

    @Autowired
    public void setScope(DecideRule scope) {
        this.scope = scope;
    }

    protected SheetOverlaysManager sheetOverlaysManager;

    public SheetOverlaysManager getSheetOverlaysManager() {
        return sheetOverlaysManager;
    }

    @Autowired
    public void setSheetOverlaysManager(
            SheetOverlaysManager sheetOverlaysManager) {
        this.sheetOverlaysManager = sheetOverlaysManager;
    }

    protected KafkaKeyedDiscardedFeed discardedUriFeed;

    public KafkaKeyedDiscardedFeed getDiscardedUriFeed() {
        return discardedUriFeed;
    }

    @Autowired
    public void setDiscardedUriFeed(KafkaKeyedDiscardedFeed discardedUriFeed) {
        this.discardedUriFeed = discardedUriFeed;
    }

    private boolean discardedUriFeedEnabled = false;

    public boolean isDiscardedUriFeedEnabled() {
        return discardedUriFeedEnabled;
    }

    public void setDiscardedUriFeedEnabled(boolean discardedUriFeedEnabled) {
        this.discardedUriFeedEnabled = discardedUriFeedEnabled;
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
     * short times (sub-5-minutes)
     */
    private Cache<String, Boolean> recentlySentCache = CacheBuilder
            .newBuilder().expireAfterWrite(5, TimeUnit.MINUTES).softValues()
            .maximumSize(2000).build();

    public void sendToKafka(String topic, CrawlURI curi, CrawlURI candidate) {
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
    protected boolean shouldEmit(CrawlURI candidate) {
        // Drop HTTP URIs that appear to be malformed:
        if (candidate.getURI().startsWith("http")) {
            try {
                String idn_host = IDN.toASCII(candidate.getUURI().getHost());
                logger.finest("Parsed URI and host successfully: " + idn_host);
            } catch (URIException e) {
                logger.warning("Could not parse URI: " + candidate.getURI());
                return false;
            } catch (IllegalArgumentException e) {
                logger.warning(
                        "Could not parse host from: " + candidate.getURI());
                return false;
            }
        }
        // Otherwise, the usual rules:
        return super.shouldEmit(candidate);
    }

    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        if (emitOutlinks) {
            // Process all outlinks:
            Collection<CrawlURI> outLinks = curi.getOutLinks();
            // Record the set of Strings in case the same URIs are extracted
            // with
            // different hop paths or contexts:
            Collection<String> sentURIs = new LinkedHashSet<String>();
            // Record what's been sent so then can be removed:
            Collection<CrawlURI> toRemove = new LinkedHashSet<CrawlURI>();

            // Iterate through the outlinks:
            for (CrawlURI candidate : outLinks) {
                // Load the sheet overlays so we apply recrawl frequencies etc.
                // (as done in CandidatesProcessor.runCandidateChain():
                candidate.setFullVia(curi);
                sheetOverlaysManager.applyOverlaysTo(candidate);
                try {
                    KeyedProperties.clearOverridesFrom(curi);
                    KeyedProperties.loadOverridesFrom(candidate);

                    // Route most via Kafka, not prerequisites
                    if (!candidate.isPrerequisite()) {

                        // Discard malformed or 'data:' or 'mailto:' URLs
                        if (this.shouldEmit(candidate)) {

                            // Avoid re-sending the same URI a lot:
                            if (!sentURIs.contains(candidate.getURI())) {

                                // Only emit URLs that are in scope, if
                                // configured
                                // to do so:
                                if (this.emitInScopeOnly) {
                                    // This part needs the
                                    // sheets/keyed-properties setup right:
                                    if (this.getScope().accepts(candidate)) {
                                        // Pass to Kafka queue:
                                        sendToKafka(getTopic(), curi,
                                                candidate);
                                    } else {
                                        // (optionally) log discarded URLs for
                                        // analysis:
                                        if (discardedUriFeedEnabled) {
                                            discardedUriFeed
                                                    .doInnerProcess(candidate);
                                        }
                                    }
                                } else {
                                    // Ignore scope rules and emit all
                                    // non-prerequisites:
                                    sendToKafka(getTopic(), curi, candidate);
                                }

                                // Record this diverted URL string so it will
                                // only
                                // be sent once:
                                sentURIs.add(candidate.getURI());

                            } else {
                                logger.finest(
                                        "Not emitting CrawlURI (appears to have been sent already): "
                                                + candidate.getURI());
                            }

                        } else {
                            logger.finest(
                                    "Not emitting CrawlURI (appears to be invalid): "
                                            + candidate.getURI());
                        }

                        // Remove all Crawl URIs except pre-requisites so only
                        // they
                        // will be queued directly:
                        toRemove.add(candidate);
                    } else {
                        logger.finest(
                                "Not emitting pre-requisite CrawlURI (will be enqueued locally): "
                                        + candidate.getURI());
                    }
                } finally {
                    KeyedProperties.clearOverridesFrom(candidate);
                    KeyedProperties.loadOverridesFrom(curi);
                }

            }

            // And remove re-routed candidates from the candidates list:
            for (CrawlURI candidate : toRemove) {
                outLinks.remove(candidate);
            }
        } else {
            // Treat the CrawlURI instead:
            CrawlURI candidate = curi;
            int statusAfterCandidateChain = candidate.getFetchStatus();

            // If the URL appears to be in-scope:
            if (statusAfterCandidateChain >= 0 ) {
                // Discard malformed or 'data:' or 'mailto:' URLs
                if (this.shouldEmit(candidate)) {
    
                    // Pass to Kafka queue:
                    sendToKafka(getTopic(), curi.getFullVia(), candidate);
    
                } else {
                    logger.finest(
                            "Not emitting CrawlURI (appears to be invalid): "
                                    + candidate.getURI());
                }
            } else {
                // (optionally) log discarded URLs for
                // analysis:
                if (discardedUriFeedEnabled) {
                    discardedUriFeed.doInnerProcess(candidate);
                }
            }
        }
    }
}