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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.openmbean.CompositeData;

import org.archive.crawler.event.CrawlURIDispositionEvent;
import org.archive.crawler.frontier.AbstractFrontier;
import org.archive.modules.CrawlURI;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;

import uk.bl.wap.crawler.frontier.RedisSimpleFrontier;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RedisFrontier extends AbstractFrontier
        implements ApplicationContextAware {

    private static final Logger logger = Logger
            .getLogger(RedisFrontier.class.getName());

    protected RedisSimpleFrontier f = new RedisSimpleFrontier();

    private int inFlight = 0;

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

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    public RedisFrontier() {
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
        return new RedisWorkQueue(curi.getClassKey());
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
        CrawlURI curi = this.f.next();
        
        // Keep a count of URIs in flight:
        inFlight++;

        return curi;
    }

    @Override
    protected void processScheduleAlways(CrawlURI caUri) {
        this.f.enqueue(caUri, true);
    }

    @Override
    protected void processScheduleIfUnique(CrawlURI caUri) {
        // TODO First check if this URL has been seen, and when it was seen:

        // TODO If not seen (or not seen within expiration time), enqueue:
        this.f.enqueue(caUri, false);
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
            this.f.dequeue(curi.getClassKey(), curi.getURI());
        }

        // Release the queue:
        this.f.releaseQueue(curi.getClassKey(),
                System.currentTimeMillis() + curi.getPolitenessDelay());
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
     * @see org.archive.crawler.frontier.AbstractFrontier#start()
     */
    @Override
    public void start() {
        super.start();
        this.f.start();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.frontier.AbstractFrontier#stop()
     */
    @Override
    public void stop() {
        super.stop();
        this.f.stop();
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    private void setQueueDelay(CrawlURI curi) {
        if (curi.includesRetireDirective()) {
            // Remove the queue from the fetch list:
            this.f.retireQueue(curi.getClassKey());
        } else {
            this.f.reschedule(curi);
        }
    }

}
