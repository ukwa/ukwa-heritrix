/**
 * 
 */
package uk.bl.wap.crawler.frontier;

import java.util.List;
import java.util.logging.Logger;

import org.archive.bdb.AutoKryo;
import org.archive.modules.CrawlURI;

import com.anotherbigidea.util.Base64;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Charsets;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;
import com.lambdaworks.redis.ScoredValue;
import com.lambdaworks.redis.ZAddArgs;

import uk.bl.wap.crawler.h3.frontier.RedisFrontier;

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
            .getLogger(RedisFrontier.class.getName());

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
    public void connect() {
        redisClient = RedisClient.create(redisEndpoint);
        connection = redisClient.connect();

        // Select the database to use:
        connection.select(redisDB);

        System.out.println("Connected to Redis");
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

    public CrawlURI next() {
        CrawlURI curi = null;

        // TODO Update/rotate 'owned' queues if required:
        // TODO Find the queue 'owned' by this instance that is due to launch
        // next:
        // TODO Pick off the next CrawlURI:
        
        // FIXME Race-condition(s)???:
        while( curi == null ) {
            long now = System.currentTimeMillis();
            System.out.println(
                    "Looking for active queues, due for processing at " + now
                            + "...");
            List<ScoredValue<String>> qs = this.connection
                    .zrangebyscoreWithScores(KEY_QS_SCHEDULED,
                    Double.NEGATIVE_INFINITY, now, 0, 1);
            String q = null;
            if (qs.size() == 0) {
                System.out.println("No queues active...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                continue;
            }
            ScoredValue<String> sq = qs.get(0);
            double score = sq.score;
            q = sq.value;
            List<String> uri = this.connection.zrangebyscore(getKeyForQueue(q),
                    -10.0, 1.0e10, 0, 1);
            if (uri.size() == 0) {
                System.out.println(
                        "No uris for queue " + q + " retiring the queue.");
                this.connection.set(KEY_QS_RETIRED, q);
                this.connection.zrem(KEY_QS_SCHEDULED, q);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                continue;
            }
            // Okay to go, so activate queue (removing from scheduled, add to
            // active):
            // FIXME Race-conditions:
            this.connection.zadd(KEY_QS_ACTIVE, score, q);
            this.connection.zrem(KEY_QS_SCHEDULED, q);
            // And log:
            System.out.println("Got URI "+uri);
            curi = getCrawlURIFromRedis(uri.get(0));
        }

        return curi;
    }

    public boolean enqueue(CrawlURI curi, boolean force) {
        String queue = curi.getClassKey();

        // FIXME The next couple of statements should really be atomic:
        long added = this.connection.zadd(getKeyForQueue(curi),
                calculateInsertKey(curi), curi.getURI());
        System.out.println("ADDED " + added);

        String result = this.connection.set("u:object:" + curi.getURI(),
                Base64.encode(caUriToKryo(curi)));
        System.out.println("RES " + result);

        // Add to available queues set, if not already active:
        Double due = this.connection.zscore(KEY_QS_SCHEDULED, queue);
        // FIXME Race-condition, but probably not a serious one (as re-adding an
        // item to the scheduled ZSET will only muck up the launch time):
        if (null == due) {
            due = (double) System.currentTimeMillis();
            Long count = this.connection.zadd(KEY_QS_SCHEDULED, due, queue);
            System.out.println("ADD " + count);
        }
        System.out.println("Is due " + (long) due.doubleValue());

        return added > 0;
    }

    public void reschedule(CrawlURI curi) {
        if (curi.includesRetireDirective()) {
            // Remove the queue from the fetch list:
            this.connection.zrem(KEY_QS_ACTIVE, curi.getClassKey());
            this.connection.set(KEY_QS_RETIRED, curi.getClassKey());
            logger.info("Queue " + curi.getClassKey() + " retired.");
            // TODO 'disown' the queue properly ???:
        } else {
            this.connection.zrem(KEY_QS_ACTIVE, curi.getClassKey());
            Long count = this.connection.zadd(KEY_QS_SCHEDULED,
                    ZAddArgs.Builder.ch(), curi.getRescheduleTime(),
                    curi.getClassKey());
            System.out.println("Updated count: " + count + " with "
                    + curi.getRescheduleTime());
            String result = this.connection.set("u:object:" + curi.getURI(),
                    Base64.encode(caUriToKryo(curi)));
            System.out.println("RES " + result);
        }
    }

    public void dequeue(String q, String uri) {
        // Remove from frontier queue
        this.connection.zrem("q:" + q + ":urls", uri);
        this.connection.del("u:object:" + uri);
    }

    public void releaseQueue(String q, Long nextFetch) {
        this.connection.zrem(KEY_QS_ACTIVE, q);
        Long count = this.connection.zadd(KEY_QS_SCHEDULED,
                ZAddArgs.Builder.ch(), nextFetch, q);
        System.out.println(
                "Updated count: " + count + " with standard crawl delay.");
    }

    public void retireQueue(String q) {
        this.connection.zrem(KEY_QS_ACTIVE, q);
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
    public void start() {
        connect();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.frontier.AbstractFrontier#stop()
     */
    public void stop() {
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
        System.out.println("Generating key for: " + q);
        return "q:" + q + ":urls";
    }

    private static String getKeyForQueue(CrawlURI curi) {
        System.out.println("Generating key for: " + curi.getClassKey());
        return "q:" + curi.getClassKey() + ":urls";
    }


    private byte[] caUriToKryo(CrawlURI curi) {
        // FIXME Not thread-safe. Need ThreadLocal instance.
        return ob.writeClassAndObject(curi);
    }

    private CrawlURI kryoToCrawlURI(byte[] buf) {
        return ob.readObject(buf, CrawlURI.class);
    }

    /**
     * Derived from BdbFrontier.
     * 
     * @see org.archive.crawler.frontier.BdbMultipleWorkQueues.
     *      calculateInsertKey(CrawlURI)
     * 
     * @param curi
     * @return
     */
    protected static long calculateInsertKey(CrawlURI curi) {
        byte[] classKeyBytes = null;
        int len = 0;
        classKeyBytes = curi.getClassKey().getBytes(Charsets.UTF_8);
        len = classKeyBytes.length;
        byte[] keyData = new byte[len+9];
        System.arraycopy(classKeyBytes,0,keyData,0,len);
        keyData[len]=0;
        long ordinalPlus = curi.getOrdinal() & 0x0000FFFFFFFFFFFFL;
        ordinalPlus = 
            ((long)curi.getSchedulingDirective() << 56) | ordinalPlus;
        long precedence = Math.min(curi.getPrecedence(), 127);
        ordinalPlus = 
            (((precedence) & 0xFFL) << 48) | ordinalPlus;
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
