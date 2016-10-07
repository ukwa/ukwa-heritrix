/**
 * 
 */
package uk.bl.wap.crawler.h3.frontier;

import static org.archive.crawler.event.CrawlURIDispositionEvent.Disposition.DEFERRED_FOR_RETRY;
import static org.archive.crawler.event.CrawlURIDispositionEvent.Disposition.DISREGARDED;
import static org.archive.crawler.event.CrawlURIDispositionEvent.Disposition.FAILED;
import static org.archive.crawler.event.CrawlURIDispositionEvent.Disposition.SUCCEEDED;
import static org.archive.modules.fetcher.FetchStatusCodes.S_DEFERRED;
import static org.archive.modules.fetcher.FetchStatusCodes.S_RUNTIME_EXCEPTION;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;

import javax.management.openmbean.CompositeData;

import org.archive.bdb.AutoKryo;
import org.archive.crawler.event.CrawlURIDispositionEvent;
import org.archive.crawler.frontier.AbstractFrontier;
import org.archive.modules.CrawlURI;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;

import com.anotherbigidea.util.Base64;
import com.esotericsoftware.kryo.ObjectBuffer;
import com.google.common.base.Charsets;
import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.RedisConnection;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RedisFrontier extends AbstractFrontier
        implements ApplicationContextAware {

    private String redisEndpoint = "redis://redis:6379";

    private int redisDB = 0;

    private RedisConnection<String, String> connection;

    private RedisClient redisClient;

    private int inFlight = 0;

    AutoKryo kryo = new AutoKryo();
    ObjectBuffer ob = new ObjectBuffer(kryo, 16 * 1024, Integer.MAX_VALUE);

    // ApplicationContextAware implementation, for eventing
    protected AbstractApplicationContext appCtx;

    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.appCtx = (AbstractApplicationContext) applicationContext;
    }

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
    void connect() {
        redisClient = RedisClient.create(redisEndpoint);
        connection = redisClient.connect();

        // Select the database to use:
        connection.select(redisDB);

        System.out.println("Connected to Redis");
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    public RedisFrontier() {
        kryo.autoregister(CrawlURI.class);
    }

    @Override
    public long discoveredUriCount() {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return 0;
    }

    @Override
    public long deepestUri() {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return 0;
    }

    @Override
    public long averageDepth() {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return 0;
    }

    @Override
    public float congestionRatio() {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return 0;
    }

    @Override
    public CompositeData getURIsList(String marker, int numberOfMatches,
            String regex, boolean verbose) {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return null;
    }

    @Override
    public long deleteURIs(String queueRegex, String match) {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return 0;
    }

    @Override
    public void deleted(CrawlURI curi) {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();

    }

    @Override
    public void considerIncluded(CrawlURI curi) {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();

    }

    @Override
    public FrontierGroup getGroup(CrawlURI curi) {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return null;
    }

    @Override
    public void reportTo(PrintWriter writer) throws IOException {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();

    }

    @Override
    public void shortReportLineTo(PrintWriter pw) throws IOException {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();

    }

    @Override
    public Map<String, Object> shortReportMap() {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return null;
    }

    @Override
    public String shortReportLegend() {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return null;
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    @Override
    protected CrawlURI findEligibleURI() {
        CrawlURI curi = null;

        // TODO Update/rotate 'owned' queues if required:
        // TODO Find the queue 'owned' by this instance that is due to launch
        // next:
        // TODO Pick off the next CrawlURI:
        
        while( curi == null ) {
            long now = System.currentTimeMillis();
            System.out.println(
                    "Looking for active queues, due for processing at " + now
                            + "...");
            List<String> qs = this.connection.zrangebyscore(KEY_QS_SCHEDULED,
                    Double.NEGATIVE_INFINITY, now, 0, 1);
            String q = null;
            if (qs.size() == 0) {
                System.out.println("No queues active...");
                String nq = this.connection.srandmember("qs:available");
                if( nq == null ) {
                    System.out.println("No queues available...");
                    continue;
                }
                this.connection.zadd(KEY_QS_SCHEDULED, now, nq);
                this.connection.srem("qs:available", nq);
                q = nq;
            } else {
                q = qs.get(0);
            }
            List<String> uri = this.connection.zrangebyscore(getKeyForQueue(q),
                    -10.0, 1.0e10, 0, 1);
            if (uri.size() == 0) {
                System.out.println("No uris for queue " + q);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                continue;
            }
            System.out.println("Got URI "+uri);
            curi = getCrawlURIFromRedis(uri.get(0));
        }

        // Keep a count of URIs in flight:
        inFlight++;

        return curi;
    }

    @Override
    protected void processScheduleAlways(CrawlURI caUri) {
        enqueue(caUri);
    }

    @Override
    protected void processScheduleIfUnique(CrawlURI caUri) {
        // TODO First check if this URL has been seen, and when it was seen:

        // TODO If not seen (or not seen within expiration time), enqueue:

    }

    @Override
    protected void processFinish(CrawlURI curi) {

        curi.incrementFetchAttempts();
        logNonfatalErrors(curi);

        int holderCost = curi.getHolderCost();

        // codes/errors which don't consume the URI, leaving it atop queue
        if (needsReenqueuing(curi)) {
            if (curi.getFetchStatus() != S_DEFERRED) {
                // wq.expend(holderCost); // all retries but DEFERRED cost
            }
            long delay_ms = retryDelayFor(curi) * 1000;
            curi.processingCleanup(); // lose state that shouldn't burden retry
            curi.setRescheduleTime(System.currentTimeMillis() + delay_ms);
            this.setQueueDelay(curi);
            if (appCtx != null) {
                appCtx.publishEvent(new CrawlURIDispositionEvent(this, curi,
                    DEFERRED_FOR_RETRY));
            }
            doJournalReenqueued(curi);
            return; // no further dequeueing, logging, rescheduling to occur
        }

        // Curi will definitely be disposed of without retry, so remove from
        // queue
        inFlight--;

        if (curi.isSuccess()) {
            // codes deemed 'success'
            incrementSucceededFetchCount();
            totalProcessedBytes.addAndGet(curi.getRecordedSize());
            if (appCtx != null) {
                appCtx.publishEvent(
                    new CrawlURIDispositionEvent(this, curi, SUCCEEDED));
            }
            doJournalFinishedSuccess(curi);

        } else if (isDisregarded(curi)) {
            // codes meaning 'undo' (even though URI was enqueued,
            // we now want to disregard it from normal success/failure tallies)
            // (eg robots-excluded, operator-changed-scope, etc)
            incrementDisregardedUriCount();
            if (appCtx != null) {
                appCtx.publishEvent(
                    new CrawlURIDispositionEvent(this, curi, DISREGARDED));
            }
            holderCost = 0; // no charge for disregarded URIs
            // TODO: consider reinstating forget-URI capability, so URI could be
            // re-enqueued if discovered again
            doJournalDisregarded(curi);

        } else {
            // codes meaning 'failure'
            incrementFailedFetchCount();
            if (appCtx != null) {
                appCtx.publishEvent(
                    new CrawlURIDispositionEvent(this, curi, FAILED));
            }
            // if exception, also send to crawlErrors
            if (curi.getFetchStatus() == S_RUNTIME_EXCEPTION) {
                Object[] array = { curi };
                loggerModule.getRuntimeErrors().log(Level.WARNING,
                        curi.getUURI().toString(), array);
            }
            // charge queue any extra error penalty
            // wq.noteError(getErrorPenaltyAmount());
            if (this.getServerCache() != null) {
                doJournalFinishedFailure(curi);
            }
        }

        // wq.expend(holderCost); // successes & failures charge cost to queue

        // TODO Update the queue next-fetch time for this queue:
        long delay_ms = curi.getPolitenessDelay();
        curi.setRescheduleTime(System.currentTimeMillis() + delay_ms);
        System.out.println("Got delay " + delay_ms);
        System.out.println("Got rescheduleTime " + curi.getRescheduleTime());

        // If it did not work, update the queue next-fetch time using the
        // CrawlURI re-schedule time:
        if (curi.getRescheduleTime() > 0) {
            // marked up for forced-revisit at a set time
            curi.processingCleanup();
            curi.resetForRescheduling();
            this.setQueueDelay(curi);
            futureUriCount.incrementAndGet();
        } else {
            curi.stripToMinimal();
            curi.processingCleanup();
            // Remove from frontier queue
            this.connection.zrem("q:" + curi.getClassKey() + ":urls",
                    curi.getURI());
            this.connection.del("u:object:" + curi.getURI());
        }

    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    @Override
    protected int getInProcessCount() {
        // TODO Auto-generated method stub
        return inFlight;
    }

    @Override
    protected long getMaxInWait() {
        // TODO Auto-generated method stub
        new Exception().printStackTrace();
        return 0;
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.frontier.AbstractFrontier#stop()
     */
    @Override
    public void stop() {
        super.stop();
        if (this.connection != null && this.connection.isOpen()) {
            this.connection.close();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.frontier.AbstractFrontier#start()
     */
    @Override
    public void start() {
        super.start();
        connect();
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    private void setQueueDelay(CrawlURI curi) {
        if (curi.includesRetireDirective()) {
            // Remove the queue from the fetch list:
            this.connection.zrem(KEY_QS_SCHEDULED,
                    curi.getClassKey());
            // TODO 'disown' the queue properly:
        } else {
            this.connection.zadd(KEY_QS_SCHEDULED,
                    curi.getRescheduleTime(),
                    curi.getClassKey());
            String result = this.connection.set("u:object:" + curi.getURI(),
                    Base64.encode(caUriToKryo(curi)));
            System.out.println("RES " + result);
        }
    }

    private static String getKeyForWorkerInfo() {
        return "w:1:info";
    }

    private static String KEY_QS_AVAILABLE = "qs:available";
    private static String KEY_QS_ACTIVE = "qs:active";
    private static String KEY_QS_SCHEDULED = "qs:scheduled";

    private static String getKeyForQueue(String q) {
        System.out.println("Generating key for: " + q);
        return "q:" + q + ":urls";
    }

    private static String getKeyForQueue(CrawlURI curi) {
        System.out.println("Generating key for: " + curi.getClassKey());
        return "q:" + curi.getClassKey() + ":urls";
    }


    private boolean enqueue(CrawlURI curi) {
        String queue = curi.getClassKey();

        // FIXME The next couple of statements should really be atomic:
        long added = this.connection.zadd(getKeyForQueue(curi),
                calculateInsertKey(curi),
                curi.getURI());
        System.out.println("ADDED " + added);

        String result = this.connection.set("u:object:" + curi.getURI(),
                Base64.encode(caUriToKryo(curi)));
        System.out.println("RES " + result);

        // Add to available queues set, if not already active:
        Double due = this.connection.zscore(KEY_QS_ACTIVE, queue);
        boolean isMember = (due == null);
        // FIXME Race-condition:
        if (!isMember) {
            this.connection.sadd("qs:available", queue);
        }
        System.out.println("RES " + result);

        return added > 0;
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
