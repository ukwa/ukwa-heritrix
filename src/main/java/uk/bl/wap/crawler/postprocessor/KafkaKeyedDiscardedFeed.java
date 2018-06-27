/**
 * 
 */
package uk.bl.wap.crawler.postprocessor;

import org.archive.modules.CrawlURI;

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
     * Allow this to be used outside of a Processor chain context:
     * 
     * @param curi
     * @throws InterruptedException
     */
    public void doInnerProcess(CrawlURI curi) throws InterruptedException {
        this.innerProcess(curi);
    }

}