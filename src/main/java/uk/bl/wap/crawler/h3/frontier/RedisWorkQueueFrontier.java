/**
 * 
 */
package uk.bl.wap.crawler.h3.frontier;

import java.util.Queue;
import java.util.SortedMap;

import javax.management.openmbean.CompositeData;

//import org.apache.commons.collections4.trie.PatriciaTrie;
import org.archive.crawler.frontier.WorkQueue;
import org.archive.crawler.frontier.WorkQueueFrontier;
import org.archive.modules.CrawlURI;

import com.sleepycat.je.DatabaseException;

import uk.bl.wap.crawler.frontier.RedisSimpleFrontier;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RedisWorkQueueFrontier extends WorkQueueFrontier {

    protected RedisSimpleFrontier f = new RedisSimpleFrontier();

    /**
     * @return the redisEndpoint
     */
    public String getRedisEndpoint() {
        return this.f.getRedisEndpoint();
    }

    /**
     * @param redisEndpoint
     *            the redisEndpoint to set, defaults to "redis://localhost:6379"
     */
    public void setRedisEndpoint(String redisEndpoint) {
        this.f.setRedisEndpoint(redisEndpoint);
    }

    /**
     * @return the DB number
     */
    public int getDB() {
        return this.f.getDB();
    }

    /**
     * @param DB
     *            the DB number to use, defaults to 0
     */
    public void setDB(int DB) {
        this.f.setDB(DB);
    }

    /* ---- */

    @Override
    public CompositeData getURIsList(String marker, int numberOfMatches, String regex, boolean verbose) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public FrontierGroup getGroup(CrawlURI curi) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected void initAllQueues() throws DatabaseException {
        this.f.connect();
    }

    @Override
    protected void initOtherQueues() throws DatabaseException {
        // TODO Auto-generated method stub

    }

    @Override
    protected SortedMap<Integer, Queue<String>> getInactiveQueuesByPrecedence() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Queue<String> createInactiveQueueForPrecedence(int precedence) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected Queue<String> getRetiredQueues() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    protected WorkQueue getQueueFor(String classKey) {
        return new RedisWorkQueue(classKey, f);
    }

    @Override
    protected boolean workQueueDataOnDisk() {
        return true;
    }

}
