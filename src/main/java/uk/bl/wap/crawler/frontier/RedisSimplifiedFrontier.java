/**
 * 
 */
package uk.bl.wap.crawler.frontier;

import java.util.List;
import java.util.logging.Logger;

import org.archive.bdb.AutoKryo;
import org.archive.modules.CrawlURI;
import org.archive.modules.SchedulingConstants;

import com.anotherbigidea.util.Base64;
import com.esotericsoftware.kryo.ObjectBuffer;

import io.lettuce.core.Limit;
import io.lettuce.core.Range;
import io.lettuce.core.RedisClient;
import io.lettuce.core.ScoredValue;
import io.lettuce.core.ZAddArgs;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

/**
 * 
 * This is a frontier implementation backed by Redis.
 * 
 * It is 'simple' in the sense that it uses basic types and is not too closely
 * tied to a particular crawl engine.
 * 
 * Note that Lettuce Redis client is thread-safe, as long as transactions are synchronized.
 * This should be fine here, as all Redis operations are in synchronized methods.
 * 
 * TODO Current version is rather too closely tied to H3 via the CrawlURI.
 * TODO Scan Redis on start up and make sure we know whatever numbers/stats we need, and make sure all the known queues are in one of scheduled/active/retired/exhausted.
 * TODO Add the distinction between exausted and retired (over quota).
 * TODO Consider switching to some kind of q:status:uk,bl, = {ACTIVE|RETIRED|ETC} so queues don't end up with mixed-up statuses.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RedisSimplifiedFrontier implements SimplifiedFrontier {

    private static final Logger logger = Logger
            .getLogger(RedisSimplifiedFrontier.class.getName());

    private String endpoint = "redis://localhost:6379";

    private int redisDB = 0;

    private RedisClient client;
    
    private StatefulRedisConnection<String, String> connection;

    private RedisCommands<String, String> commands; 

    static private AutoKryo kryo = new AutoKryo();
    static private final ThreadLocal<ObjectBuffer> obs = new ThreadLocal<ObjectBuffer>() {
	   protected ObjectBuffer initialValue() {
		  ObjectBuffer ob = new ObjectBuffer(kryo, 16 * 1024, Integer.MAX_VALUE);
	      return ob;
	   };
	};
    
	public class FrontierEmptyException extends Exception {

		/**
		 * @param string
		 */
		public FrontierEmptyException(String string) {
			super(string);
		}

		/**
		 * 
		 */
		private static final long serialVersionUID = 5194808787216022781L;
		
	}
    

    /**
     * @return the redisEndpoint
     */
    public String getEndpoint() {
        return endpoint;
    }

    /**
     * @param redisEndpoint
     *            the redisEndpoint to set, defaults to "redis://redis:6379"
     */
    public void setEndpoint(String redisEndpoint) {
        this.endpoint = redisEndpoint;
    }

    /**
     * @return the DB number
     */
    public int getDB() {
        return redisDB;
    }

    /**
     * @param DB
     *            the DB number to use, defaults to 0
     */
    public void setDB(int DB) {
        this.redisDB = DB;
    }

    /**
     * 
     */
    public synchronized void connect() {
    	if( this.connection == null || !this.connection.isOpen() ) {
	        client = RedisClient.create(endpoint);
	        connection = client.connect();
	        commands = connection.sync();
	
	        // Select the database to use:
	        commands.select(redisDB);
	
	        logger.info("Connected to Redis");
    	}
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    public RedisSimplifiedFrontier() {
        kryo.autoregister(CrawlURI.class);
    }


    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

	/**
     * Wait for a URL to be due.
     * 
     * If no more are scheduled return null.
     * 
     * @return
     */
    @Override
    public CrawlURI next() {
        CrawlURI curi = null;

        // TODO Update/rotate 'owned' queues if required:
        // TODO Find the queue 'owned' by this instance that is due to launch
        // next:

        // TODO Pick off the next CrawlURI:
        while (curi == null) {
            try {
                curi = this.due();
            } catch (FrontierEmptyException e) {
            	// No URLs 	queue at all:
                return null;
            }
            // Sleep if there's nothing due...
            if (curi == null) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }

        // return what we've got:
        return curi;
    }

    /**
     * 
     * If there is a URL due to be crawled, return it.
     * 
     * If there are URLs scheduled to be crawled, but not yet, return null.
     * 
     * If there are no URLs scheduled to be crawled, throw an Exception.
     * 
     * FIXME Race-conditions, as in Redis could end up inconsistent if this
     * method dies mid-flow.
     * 
     * SCHEDULED -> ACTIVE
     * 
     * @return
     * @throws Exception
     */
    private synchronized CrawlURI due() throws FrontierEmptyException {
    	// Look for the first scheduled queue that is due:
        long now = System.currentTimeMillis();
        logger.finest("Looking for active queues, due for processing at " + now
                + "...");
        List<ScoredValue<String>> qs = this.commands.zrangebyscoreWithScores(
                KEY_QS_SCHEDULED, Range.create(Double.NEGATIVE_INFINITY, now), Limit.create(0, 1));
        String q = null;
        if (qs.size() == 0) {
            long totalScheduled = this.commands.zcard(KEY_QS_SCHEDULED);
            long totalActive = this.commands.zcard(KEY_QS_ACTIVE);
            if ((totalScheduled + totalActive) > 0) {
                return null;
            } else {
                logger.finest("No queues scheduled to run.");
                throw new FrontierEmptyException("No more URLs scheduled!");
            }
        }
        
        // Now find the highest-priority (lowest-score) URL for that queue:
        ScoredValue<String> sq = qs.get(0);
        double score = sq.getScore();
        q = sq.getValue();
        List<String> uri = this.commands.zrangebyscore(getKeyForQueue(q),
                Range.create(-10.0, 1.0e10), Limit.create(0, 1));
        if (uri.size() == 0) {
            logger.info("No uris for queue " + q + " exhausting the queue.");
            this.commands.zrem(KEY_QS_SCHEDULED, q);
            this.commands.sadd(KEY_QS_EXHAUSTED, q);
            return null;
        }
        // Okay to go, so activate queue (removing from scheduled, add to
        // active):
        this.commands.multi();
        this.commands.zrem(KEY_QS_SCHEDULED, q);
        this.commands.zadd(KEY_QS_ACTIVE, score, q);
        this.commands.exec();
        // And log:
        logger.fine("Got URI " + uri);
        CrawlURI curi = getCrawlURIFromRedis(uri.get(0));
        if (curi == null) {
            throw new RuntimeException("Frontier damaged, CrawlURI for " + uri
                    + " cannot be found!");
        }
        return curi;
    }

    /**
     * TODO Wrap all this in a transaction?
     * 
     * @param curi
     * @return
     */
    @Override
    public synchronized boolean enqueue(String queue, CrawlURI curi) {
        // Enqueue it, if the URL is already there, only update it if 
        // the new score is less than the current score:
        // (see https://redis.io/commands/ZADD)
        long added = this.commands.zadd(getKeyForQueue(queue), ZAddArgs.Builder.lt(),
                calculateScore(curi),  curi.getURI());
        logger.finer("Queued " + added + " of " + curi);

        // Store the CrawlURI data, but only if the update worked:
        if (added > 0) {
	        String urlKey = KEY_OP_URI + curi.getURI();
	        String result = this.commands.set(urlKey,
	                Base64.encode(caUriToKryo(curi)));
	        logger.finer("Object " + result + " stored " + curi);
        }

        // Add to available queues set, if not already active:
        Double due = (double) System.currentTimeMillis();
        long addedQ = this.commands.zadd(
        		KEY_QS_SCHEDULED, 
        		ZAddArgs.Builder.nx(), // NX == don't update existing elements
                due, 
                queue);
    	// If this is a new queue, it got an immediate start time:
        if (addedQ > 0) {
            logger.finer("Queue new, set due " + due + " for queue " + queue);
        	// Add to a list of all known queues:
        	this.commands.sadd(KEY_QS_KNOWN, queue);
        } else {
            // Remove this queue from other sets, if it's there:
        	this.commands.srem(KEY_QS_RETIRED, queue);
        	this.commands.srem(KEY_QS_EXHAUSTED, queue);
        }

        return added > 0;
    }

	@Override
    public synchronized void dequeue(String q, String uri) {
    	logger.finest("Dequeuing "+  uri + " from queue " + q);
        // Remove from frontier queue
    	this.commands.multi();
        this.commands.del(KEY_OP_URI + uri);
        this.commands.zrem(getKeyForQueue(q), uri);
        this.commands.exec();
    }

    /**
     * 
     * ACTIVE -> SCHEDULED
     * 
     * @param q
     * @param fetchTime
     */
    @Override
    public synchronized void delayQueue(String q, long fetchTime) {
    	// TODO Wrap this in a transaction?
        Long count = this.commands.zadd(KEY_QS_SCHEDULED,
                ZAddArgs.Builder.ch(), fetchTime,
                q);
        this.commands.zrem(KEY_QS_ACTIVE, q);
        logger.finer("Updated count: " + count + " queue " + q + " with " + fetchTime);
    }

    /**
     * {ANY} -> RETIRED
     * 
     * @param q
     */
	@Override
    public synchronized void retireQueue(String q) {
    	this.commands.multi();
        this.commands.zrem(KEY_QS_ACTIVE, q);
        this.commands.zrem(KEY_QS_SCHEDULED, q);
        this.commands.zrem(KEY_QS_EXHAUSTED, q);
        this.commands.sadd(KEY_QS_RETIRED, q);
        this.commands.exec();
        logger.finer("Queue " + q + " retired.");
        // TODO 'disown' the queue properly ???:
    }
    
	@Override
	public boolean isRunning() {
    	if( this.commands != null && this.commands.isOpen()) {
    		return true;
    	}
    	return false;
    }
    
	@Override
    public long getTotalQueues() {
    	return this.commands.scard(KEY_QS_KNOWN);
    }
    
    @Override
    public long getActiveQueues() {
    	return this.commands.zcard(KEY_QS_ACTIVE);
    }
    @Override
    public long getScheduledQueues() {
    	return this.commands.zcard(KEY_QS_SCHEDULED);
    }
    @Override
    public long getRetiredQueues() {
    	return this.commands.scard(KEY_QS_RETIRED);
    }
 	@Override
    public long getExhaustedQueues() {
    	return this.commands.scard(KEY_QS_EXHAUSTED);
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

	/*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.frontier.AbstractFrontier#start()
     */
    @Override
    public synchronized void start() {
        connect();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.frontier.AbstractFrontier#stop()
     */
    @Override
    public synchronized void stop() {
        if (this.connection != null && this.connection.isOpen()) {
            this.connection.close();
            this.client.shutdown();                                                     
        }
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    private static String KEY_QS_SCHEDULED = "qs:scheduled";
    private static String KEY_QS_ACTIVE = "qs:active";
    private static String KEY_QS_EXHAUSTED = "qs:exhausted";
    private static String KEY_QS_RETIRED = "qs:retired";
    private static String KEY_QS_KNOWN = "qs:known";
    private static String KEY_OP_URI = "u:object:"; // URI Object prefix.

    private static String getKeyForQueue(String q) {
        logger.finest("Generating key for: " + q);
        return "q:urls:" + q;
    }
    
    private static String getQueueKey(String q) {
    	// Replace commas with colons in key so they can be interpreted as a tree:
    	// TODO Use this instead of getClassKey.
    	// TODO Is this really a good idea?
    	return q.replace(",", ":");
    }

    private synchronized byte[] caUriToKryo(CrawlURI curi) {
        return obs.get().writeClassAndObject(curi);
    }

    private synchronized CrawlURI kryoToCrawlURI(byte[] buf) {
        return obs.get().readObject(buf, CrawlURI.class);
    }

    /**
     * 
     * @see SchedulingConstants.HIGHEST = 0, SchedulingConstants.NORMAL = 3 and
     *      Precedence mean the higher the number the less important (1 is
     *      highest)
     * 
     *      Redis sorts low-to-high by default, so (schedulingConstant << 8) &&
     *      Precedence should work well.
     * 
     * @param curi
     * @return
     */
    protected static double calculateScore(CrawlURI curi) {
        logger.finest("Calculating score/priority for " + curi + " "
                + curi.getSchedulingDirective() + " " + curi.getPrecedence());
        double precedence = (curi.getSchedulingDirective() << 8)
                + curi.getPrecedence();
        logger.finest(
                "Calculated score/priority for " + curi + " = " + precedence);
        return precedence;
    }
    
    private CrawlURI getCrawlURIFromRedis(String uri) {
        String object = this.commands.get(KEY_OP_URI + uri);
        try {
            return this.kryoToCrawlURI(Base64.decode(object));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

}
