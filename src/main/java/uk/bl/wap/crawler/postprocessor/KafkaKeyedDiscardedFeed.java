/**
 * 
 */
package uk.bl.wap.crawler.postprocessor;

import java.util.concurrent.TimeUnit;

import org.archive.modules.CrawlURI;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

/**
 * 
 * Sub-class that can be used in at the end of Candidate chains to capture
 * out-of-scope URLs
 * 
 * Based on: @see AMQPPublishProcessor and @see KafkaKeyedCrawlLogFeed
 * and @KafkaUrlReceiver
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class KafkaKeyedDiscardedFeed extends KafkaKeyedCrawlLogFeed {

    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        int statusAfterCandidateChain = curi.getFetchStatus();
        // The fetch-status will be < 0 if the candidates chain rejected this
        // CrawlURI:
        if (statusAfterCandidateChain < 0) {
            return true;
        }
        return false;
    }

    /**
     * Use a cache to avoid re-sending the same discarded URLs multiple times
     * over short periods of time.
     */
    private Cache<String, Boolean> recentlySentCache = CacheBuilder.newBuilder()
            .expireAfterWrite(5, TimeUnit.MINUTES).softValues()
            .maximumSize(2000).build();

    /**
     * Allow this to be used outside of a Processor chain context:
     * 
     * @param curi
     * @throws InterruptedException
     */
    public void doInnerProcess(CrawlURI curi) throws InterruptedException {
        // Check if this URL has been sent recently:
        Boolean recentlySent = recentlySentCache.getIfPresent(curi.getURI());
        if (recentlySent == null) {
            this.innerProcess(curi);
            logger.finest("Sending discarded URL: " + curi + " via "
                    + curi.flattenVia());
            recentlySentCache.put(curi.getURI(), true);
        } else {
            logger.finest("Ignoring recently-sent discarded URL: " + curi
                    + " via " + curi.flattenVia());
        }
    }

}