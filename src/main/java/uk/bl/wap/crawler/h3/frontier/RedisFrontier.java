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

import org.archive.crawler.datamodel.UriUniqFilter;
import org.archive.crawler.event.CrawlURIDispositionEvent;
import org.archive.crawler.frontier.AbstractFrontier;
import org.archive.modules.CrawlURI;
import org.archive.spring.KeyedProperties;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
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

    private long discoveredUrisCount = 0;

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

    /**
     * Number of <i>discovered</i> URIs.
     *
     * <p>
     * That is any URI that has been confirmed be within 'scope' (i.e. the
     * Frontier decides that it should be processed). This includes those that
     * have been processed, are being processed and have finished processing.
     * Does not include URIs that have been 'forgotten' (deemed out of scope
     * when trying to fetch, most likely due to operator changing scope
     * definition).
     *
     * <p>
     * <b>Note:</b> This only counts discovered URIs. Since the same URI can (at
     * least in most frontiers) be fetched multiple times, this number may be
     * somewhat lower then the combined <i>queued</i>, <i>in process</i> and
     * <i>finished</i> items combined due to duplicate URIs being queued and
     * processed. This variance is likely to be especially high in Frontiers
     * implementing 'revist' strategies.
     *
     * @return Number of discovered URIs.
     */
    @Override
    public long discoveredUriCount() {
        return this.discoveredUrisCount;
    }

    /**
     * Ordinal position of the 'deepest' URI eligible for crawling. Essentially,
     * the length of the longest frontier internal queue.
     * 
     * @return long URI count to deepest URI
     */
    @Override
    public long deepestUri() {
        // TODO Calculate this properly.
        return 1;
    }

    /**
     * Average depth of the last URI in all eligible queues. That is, the
     * average length of all eligible queues.
     * 
     * @return long average depth of last URIs in queues
     */
    @Override
    public long averageDepth() {
        // TODO Calculate this properly.
        return 1;
    }

    /**
     * Ratio of number of threads that would theoretically allow maximum crawl
     * progress (if each was as productive as current threads), to current
     * number of threads.
     * 
     * @return float congestion ratio
     */
    @Override
    public float congestionRatio() {
        // TODO Calculate this properly.
        return 1;
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
        return new RedisWorkQueue(curi.getClassKey(), f);
    }

    /**
     * @see org.archive.crawler.frontier.WorkQueueFrontier.reportTo(PrintWriter)
     */
    @Override
    public void reportTo(PrintWriter writer) throws IOException {
        // FIXME Write basic report on what the frontier saw.
        writer.println("Redis Frontier Report");

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

    /**
     * The UriUniqFilter to use, tracking those UURIs which are already
     * in-process (or processed), and thus should not be rescheduled. Also known
     * as the 'alreadyIncluded' or 'alreadySeen' structure
     */
    protected UriUniqFilter uriUniqFilter;

    public UriUniqFilter getUriUniqFilter() {
        return this.uriUniqFilter;
    }

    @Autowired
    public void setUriUniqFilter(UriUniqFilter uriUniqFilter) {
        this.uriUniqFilter = uriUniqFilter;
        this.uriUniqFilter.setDestination(this);
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    /**
     * 
     * Waits until it's time to fetch the next thing.
     * 
     * But should return NULL if there is nothing to crawl at all (in the
     * future).
     * 
     * @return
     */
    @Override
    protected CrawlURI findEligibleURI() {
        CrawlURI curi = this.f.next();

        logger.finest("Returning: " + curi);
        // Set the number 'inFlight' to zero, so the crawl can end.
        if (curi == null) {
            this.inFlight = 0;
        } else {
            // Ensure sheet overlays are applied:
            sheetOverlaysManager.applyOverlaysTo(curi);
            try {
                KeyedProperties.loadOverridesFrom(curi);
                // curi.setSessionBudget(getBalanceReplenishAmount());
                // curi.setTotalBudget(getQueueTotalBudget());
            } finally {
                KeyedProperties.clearOverridesFrom(curi);
            }
        }
        return curi;
    }

    /**
     * Arrange for the given CrawlURI to be visited, if it is not already
     * enqueued/completed.
     * 
     * Oddly the inherited version of this fails to actually schedule!
     *
     * @see org.archive.crawler.framework.Frontier#schedule(org.archive.modules.CrawlURI)
     */
    @Override
    public void schedule(CrawlURI curi) {
        sheetOverlaysManager.applyOverlaysTo(curi);
        try {
            KeyedProperties.loadOverridesFrom(curi);
            if (curi.getClassKey() == null) {
                // remedial processing
                preparer.prepare(curi);
            }
            processScheduleIfUnique(curi);
        } finally {
            KeyedProperties.clearOverridesFrom(curi);
        }
    }

    @Override
    protected void processScheduleAlways(CrawlURI curi) {
        assert KeyedProperties.overridesActiveFrom(curi);

        this.discoveredUrisCount++;
        this.inFlight++;
        this.f.enqueue(curi);
    }

    @Override
    protected void processScheduleIfUnique(CrawlURI curi) {
        assert KeyedProperties.overridesActiveFrom(curi);

        // Canonicalization may set forceFetch flag. See
        // #canonicalization(CrawlURI) javadoc for circumstance.
        String canon = curi.getCanonicalString();
        if (curi.forceFetch()) {
            uriUniqFilter.addForce(canon, curi);
        } else {
            uriUniqFilter.add(canon, curi);
        }
    }

    @Override
    protected void processFinish(CrawlURI curi) {

        curi.incrementFetchAttempts();
        logNonfatalErrors(curi);

        int holderCost = curi.getHolderCost();

        // codes/errors which don't consume the URI, leaving it atop queue
        if (needsReenqueuing(curi)) {
            logger.finest("Re-enqueing " + curi + " " + curi.getFetchStatus());
            if (curi.getFetchStatus() != S_DEFERRED) {
                // wq.expend(holderCost); // all retries but DEFERRED cost
            }
            long delay_ms = retryDelayFor(curi) * 1000;
            curi.processingCleanup(); // lose state that shouldn't burden retry
            this.setQueueDelay(curi, System.currentTimeMillis() + delay_ms);
            if (appCtx != null) {
                appCtx.publishEvent(new CrawlURIDispositionEvent(this, curi,
                    DEFERRED_FOR_RETRY));
            }
            doJournalReenqueued(curi);
            return; // no further dequeueing, logging, rescheduling to occur
        }

        // Curi will definitely be disposed of without retry, so remove from
        // queue
        this.delete(curi);

        if (curi.isSuccess()) {
            logger.finest("SUCCESS " + curi);
            // codes deemed 'success'
            incrementSucceededFetchCount();
            totalProcessedBytes.addAndGet(curi.getRecordedSize());
            if (appCtx != null) {
                appCtx.publishEvent(
                    new CrawlURIDispositionEvent(this, curi, SUCCEEDED));
            }
            doJournalFinishedSuccess(curi);

        } else if (isDisregarded(curi)) {
            logger.finest("DISREGARD " + curi);
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
            logger.finest("FAILED " + curi);
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

        // Update the queue next-fetch time for this queue:
        long delay_ms = curi.getPolitenessDelay();
        // Release the queue:
        this.f.releaseQueue(curi.getClassKey(),
                System.currentTimeMillis() + delay_ms);
        logger.finest("Got delay " + delay_ms);
        logger.finest("Got rescheduleTime " + curi.getRescheduleTime());

        // If it did not work, update the queue next-fetch time using the
        // CrawlURI re-schedule time:
        if (curi.getRescheduleTime() > 0) {
            // marked up for forced-revisit at a set time
            curi.processingCleanup();
            curi.resetForRescheduling();
            this.setQueueDelay(curi, curi.getRescheduleTime());
            futureUriCount.incrementAndGet();
        } else {
            curi.stripToMinimal();
            curi.processingCleanup();
            inFlight--;
        }

    }

    protected void delete(CrawlURI curi) {
        this.f.dequeue(curi.getClassKey(), curi.getURI());
    }

    /* ------- ------- ------- ------- ------- ------- ------- ------- */
    /* */
    /* ------- ------- ------- ------- ------- ------- ------- ------- */

    /**
     * The number of CrawlURIs 'in process' (passed to the outbound queue and
     * not yet finished by returning through the inbound queue.)
     * 
     * @return number of in-process CrawlURIs
     */
    @Override
    protected int getInProcessCount() {
        logger.fine("Current inFlight = " + inFlight);
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

    private void setQueueDelay(CrawlURI curi, long fetchTime) {
        if (curi.includesRetireDirective()) {
            // Remove the queue from the fetch list:
            this.f.retireQueue(curi.getClassKey());
        } else {
            if (fetchTime == -1) {
                fetchTime = curi.getRescheduleTime();
            }
            this.f.reschedule(curi, fetchTime);
        }
    }

}
