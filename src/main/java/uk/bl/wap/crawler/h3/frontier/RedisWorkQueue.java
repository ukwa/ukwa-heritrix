/**
 * 
 */
package uk.bl.wap.crawler.h3.frontier;

import java.io.IOException;

import org.archive.crawler.frontier.WorkQueue;
import org.archive.crawler.frontier.WorkQueueFrontier;
import org.archive.modules.CrawlURI;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RedisWorkQueue extends WorkQueue {

    /**
     * 
     */
    private static final long serialVersionUID = 7310952064588710312L;

    public RedisWorkQueue(String pClassKey) {
        super(pClassKey);
        // TODO Auto-generated constructor stub
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#insertItem(org.archive.crawler.frontier.WorkQueueFrontier, org.archive.modules.CrawlURI, boolean)
     */
    @Override
    protected void insertItem(WorkQueueFrontier frontier, CrawlURI curi,
            boolean overwriteIfPresent) throws IOException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#deleteMatchingFromQueue(org.archive.crawler.frontier.WorkQueueFrontier, java.lang.String)
     */
    @Override
    protected long deleteMatchingFromQueue(WorkQueueFrontier frontier,
            String match) throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#deleteItem(org.archive.crawler.frontier.WorkQueueFrontier, org.archive.modules.CrawlURI)
     */
    @Override
    protected void deleteItem(WorkQueueFrontier frontier, CrawlURI item)
            throws IOException {
        // TODO Auto-generated method stub

    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#peekItem(org.archive.crawler.frontier.WorkQueueFrontier)
     */
    @Override
    protected CrawlURI peekItem(WorkQueueFrontier frontier) throws IOException {
        // TODO Auto-generated method stub
        return null;
    }

}
