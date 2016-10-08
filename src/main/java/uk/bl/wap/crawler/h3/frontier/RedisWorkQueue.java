/**
 * 
 */
package uk.bl.wap.crawler.h3.frontier;

import java.io.IOException;

import org.archive.crawler.frontier.WorkQueue;
import org.archive.crawler.frontier.WorkQueueFrontier;
import org.archive.modules.CrawlURI;

import uk.bl.wap.crawler.frontier.RedisSimpleFrontier;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RedisWorkQueue extends WorkQueue {

    private final String queue;

    /**
     * 
     */
    private static final long serialVersionUID = 7310952064588710312L;

    private final RedisSimpleFrontier f;

    public RedisWorkQueue(String pClassKey, RedisSimpleFrontier rsf) {
        super(pClassKey);
        this.queue = pClassKey;
        this.f = rsf;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#insertItem(org.archive.crawler.frontier.WorkQueueFrontier, org.archive.modules.CrawlURI, boolean)
     */
    @Override
    protected void insertItem(WorkQueueFrontier frontier, CrawlURI curi,
            boolean overwriteIfPresent) throws IOException {
        f.enqueue(curi, overwriteIfPresent);
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
        f.dequeue(queue, item.getURI());

    }

    /* (non-Javadoc)
     * @see org.archive.crawler.frontier.WorkQueue#peekItem(org.archive.crawler.frontier.WorkQueueFrontier)
     */
    @Override
    protected CrawlURI peekItem(WorkQueueFrontier frontier) throws IOException {
        CrawlURI curi = null;
        
        try {
            curi = frontier.next();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
                
        return curi;
    }

}
