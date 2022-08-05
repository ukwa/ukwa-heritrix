/**
 * 
 */
package uk.bl.wap.crawler.h3.frontier;

import java.io.IOException;

import org.archive.crawler.frontier.WorkQueue;
import org.archive.crawler.frontier.WorkQueueFrontier;
import org.archive.modules.CrawlURI;
import org.archive.util.ObjectIdentityMemCache;

import uk.bl.wap.crawler.frontier.RedisSimplifiedFrontier;

/**
 * 
 * As we are not using WorkQueue in the frontier itself, this is only 
 * needed to manage queue stats/tallies etc. It is not used to actually 
 * manage the crawl.
 * 
 * FIXME These stats etc. should really be pushed into the SimplifiedFrontier implementation.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SimplifiedFrontierWorkQueue extends WorkQueue {

    private final String queue;

    /**
     * 
     */
    private static final long serialVersionUID = 7310952064588710312L;

    public SimplifiedFrontierWorkQueue(String pClassKey) {
        super(pClassKey);
        this.queue = pClassKey;
        // We don't use this, but the tally() code hits it via
        // org.archive.crawler.frontier.WorkQueue.makeDirty(WorkQueue.java:690)
        this.setIdentityCache(new ObjectIdentityMemCache<WorkQueue>());
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#insertItem(org.archive.crawler.frontier.WorkQueueFrontier, org.archive.modules.CrawlURI, boolean)
     */
    @Override
    protected void insertItem(WorkQueueFrontier frontier, CrawlURI curi,
            boolean overwriteIfPresent) throws IOException {
        new Exception().printStackTrace();
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#deleteMatchingFromQueue(org.archive.crawler.frontier.WorkQueueFrontier, java.lang.String)
     */
    @Override
    protected long deleteMatchingFromQueue(WorkQueueFrontier frontier,
            String match) throws IOException {
        new Exception().printStackTrace();
        return 0;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#deleteItem(org.archive.crawler.frontier.WorkQueueFrontier, org.archive.modules.CrawlURI)
     */
    @Override
    protected void deleteItem(WorkQueueFrontier frontier, CrawlURI item)
            throws IOException {
        new Exception().printStackTrace();
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#peekItem(org.archive.crawler.frontier.WorkQueueFrontier)
     */
    @Override
    protected CrawlURI peekItem(WorkQueueFrontier frontier) throws IOException {
        new Exception().printStackTrace();
        return null;
    }

}
