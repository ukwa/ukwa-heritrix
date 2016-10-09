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
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.ScoredValue;
import com.lambdaworks.redis.ZAddArgs;

/**
 * 
 * This is a frontier implementation backed by Redis.
 * 
 * It is 'simple' in the sense that it uses basic types and is not too closely
 * tied to a particular crawl engine.
 * 
 * FIXME Current version is rather too closely tied to H3 via the CrawlURI.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RedisSimpleFrontier {

    private static final Logger logger = Logger
            .getLogger(RedisSimpleFrontier.class.getName());

    private String redisEndpoint = "redis://localhost:6379";

    private int redisDB = 0;

    private RedisConnection<String, String> connection;

    private RedisClient redisClient;

    AutoKryo kryo = new AutoKryo();
    ObjectBuffer ob = new ObjectBuffer(kryo, 16 * 1024, Integer.MAX_VALUE);

    /**
     * @return the redisEndpoint
     */
    public String getRedisEndpoint() {
        return redisEndpoint;
    }

    /**
     * @param redisEndpoint
     *            the redisEndpoint to set, defaults to "redis://redis:6379"
     */
    public void setRedisEndpoint(String redisEndpoint) {
        this.redisEndpoint = redisEndpoint;
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
        redisClient = RedisClient.create(redisEndpoint);
        connection = redisClient.connect();

        // Select the database to use:
        connection.select(redisDB);

        logger.info("Connected to Redis");
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    public RedisSimpleFrontier() {
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
    public CrawlURI next() {
        CrawlURI curi = null;

        // TODO Update/rotate 'owned' queues if required:
        // TODO Find the queue 'owned' by this instance that is due to launch
        // next:

        // TODO Pick off the next CrawlURI:
        while (curi == null) {
            try {
                curi = this.due();
            } catch (Exception e) {
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
     * @return
     * @throws Exception
     */
    public synchronized CrawlURI due() throws Exception {
        long now = System.currentTimeMillis();
        logger.finest("Looking for active queues, due for processing at " + now
                + "...");
        List<ScoredValue<String>> qs = this.connection.zrangebyscoreWithScores(
                KEY_QS_SCHEDULED, Double.NEGATIVE_INFINITY, now, 0, 1);
        String q = null;
        if (qs.size() == 0) {
            long totalScheduled = this.connection.zcard(KEY_QS_SCHEDULED);
            long totalActive = this.connection.zcard(KEY_QS_ACTIVE);
            if ((totalScheduled + totalActive) > 0) {
                return null;
            } else {
                logger.finer("No queues scheduled to run.");
                throw new Exception("No more URLs scheduled!");
            }
        }
        ScoredValue<String> sq = qs.get(0);
        double score = sq.score;
        q = sq.value;
        List<String> uri = this.connection.zrangebyscore(getKeyForQueue(q),
                -10.0, 1.0e10, 0, 1);
        if (uri.size() == 0) {
            logger.info("No uris for queue " + q + " retiring the queue.");
            this.connection.set(KEY_QS_RETIRED, q);
            this.connection.zrem(KEY_QS_SCHEDULED, q);
            return null;
        }
        // Okay to go, so activate queue (removing from scheduled, add to
        // active):
        this.connection.zadd(KEY_QS_ACTIVE, score, q);
        this.connection.zrem(KEY_QS_SCHEDULED, q);
        // And log:
        logger.fine("Got URI " + uri);
        CrawlURI curi = getCrawlURIFromRedis(uri.get(0));
        if (curi == null) {
            throw new Exception("Frontier damaged, CrawlURI for " + uri
                    + " cannot be found!");
        }
        return curi;
    }

    public synchronized boolean enqueue(CrawlURI curi) {
        String urlKey = "u:object:" + curi.getURI();
        String queue = curi.getClassKey();

        // FIXME The next couple of statements should really be atomic:
        long added = this.connection.zadd(getKeyForQueue(curi),
                calculateInsertKey(curi), curi.getURI());
        logger.finest("ADDED " + added + " for " + curi);

        // Also store the URI itself:
        String result = this.connection.set(urlKey,
                Base64.encode(caUriToKryo(curi)));
        logger.finest("RES " + result + " stored " + curi);

        // Add to available queues set, if not already active:
        Double due = this.connection.zscore(KEY_QS_SCHEDULED, queue);
        // FIXME Race-condition, but probably not a serious one (as re-adding an
        // item to the scheduled ZSET will only muck up the launch time):
        if (null == due) {
            due = (double) System.currentTimeMillis();
            Long count = this.connection.zadd(KEY_QS_SCHEDULED, due, queue);
            logger.finest("ADD " + count + " for uri " + curi);
        }
        logger.finest("Enqueued URI is due " + (long) due.doubleValue());

        return added > 0;
    }

    public synchronized void reschedule(CrawlURI curi, long fetchTime) {
        if (curi.includesRetireDirective()) {
            // Remove the queue from the fetch list:
            this.connection.zrem(KEY_QS_ACTIVE, curi.getClassKey());
            this.connection.set(KEY_QS_RETIRED, curi.getClassKey());
            logger.info("Queue " + curi.getClassKey() + " retired.");
            // TODO 'disown' the queue properly ???:
        } else {
            this.connection.zrem(KEY_QS_ACTIVE, curi.getClassKey());
            Long count = this.connection.zadd(KEY_QS_SCHEDULED,
                    ZAddArgs.Builder.ch(), fetchTime,
                    curi.getClassKey());
            logger.finest("Updated count: " + count + " with " + fetchTime);
            String result = this.connection.set("u:object:" + curi.getURI(),
                    Base64.encode(caUriToKryo(curi)));
            logger.finest("RES " + result + " updated object for " + curi);
        }
    }

    public synchronized void dequeue(String q, String uri) {
        // Remove from frontier queue
        this.connection.zrem("q:" + q + ":urls", uri);
        this.connection.del("u:object:" + uri);
    }

    public synchronized void releaseQueue(String q, Long nextFetch) {
        this.connection.zrem(KEY_QS_ACTIVE, q);
        Long count = this.connection.zadd(KEY_QS_SCHEDULED,
                ZAddArgs.Builder.ch(), nextFetch, q);
        logger.finest(
                "ReleaseQueue updated count: " + count + " until " + nextFetch);
    }

    public synchronized void retireQueue(String q) {
        this.connection.zrem(KEY_QS_ACTIVE, q);
        this.connection.zrem(KEY_QS_SCHEDULED, q);
        this.connection.set(KEY_QS_RETIRED, q);
        logger.info("Queue " + q + " retired.");
        // TODO 'disown' the queue properly ???:
    }


    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.frontier.AbstractFrontier#start()
     */
    public synchronized void start() {
        connect();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.frontier.AbstractFrontier#stop()
     */
    public synchronized void stop() {
        if (this.connection != null && this.connection.isOpen()) {
            this.connection.close();
        }
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    private static String getKeyForWorkerInfo() {
        return "w:1:info";
    }

    private static String KEY_QS_SCHEDULED = "qs:scheduled";
    private static String KEY_QS_ACTIVE = "qs:active";
    private static String KEY_QS_RETIRED = "qs:retired";

    private static String getKeyForQueue(String q) {
        logger.finest("Generating key for: " + q);
        return "q:" + q + ":urls";
    }

    private static String getKeyForQueue(CrawlURI curi) {
        logger.finest("Generating key for: " + curi.getClassKey());
        return "q:" + curi.getClassKey() + ":urls";
    }


    private synchronized byte[] caUriToKryo(CrawlURI curi) {
        // FIXME Not thread-safe. Need ThreadLocal instance.
        return ob.writeClassAndObject(curi);
    }

    private synchronized CrawlURI kryoToCrawlURI(byte[] buf) {
        return ob.readObject(buf, CrawlURI.class);
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
    protected static double calculateInsertKey(CrawlURI curi) {
        logger.finest("Calculating insertion key for " + curi + " "
                + curi.getSchedulingDirective() + " " + curi.getPrecedence());
        double precedence = (curi.getSchedulingDirective() << 8)
                + curi.getPrecedence();
        logger.finest(
                "Calculated insertion key for " + curi + " = " + precedence);
        return precedence;
    }
    
    private CrawlURI getCrawlURIFromRedis(String uri) {
        String object = this.connection.get("u:object:" + uri);
        try {
            return this.kryoToCrawlURI(Base64.decode(object));
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return null;
    }

}
