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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.management.openmbean.CompositeData;

import org.archive.crawler.datamodel.UriUniqFilter;
import org.archive.crawler.event.CrawlURIDispositionEvent;
import org.archive.crawler.frontier.AbstractFrontier;
import org.archive.crawler.frontier.WorkQueue;
import org.archive.modules.CrawlURI;
import org.archive.spring.KeyedProperties;
import org.archive.util.ObjectIdentityCache;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.AbstractApplicationContext;

import com.sleepycat.collections.StoredSortedMap;

import uk.bl.wap.crawler.frontier.RedisSimpleFrontier;

/**
 * 
 * FIXME 167 downloaded + -8 queued = 159 total  BUT 175 entries in the crawl log!
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RedisFrontier extends AbstractFrontier
        implements ApplicationContextAware {

    private static final Logger logger = Logger
            .getLogger(RedisFrontier.class.getName());

    protected RedisSimpleFrontier f = new RedisSimpleFrontier();

    protected AtomicInteger inFlight = new AtomicInteger(0);

    protected AtomicLong discoveredUrisCount = new AtomicLong(0);

    // ApplicationContextAware implementation, for eventing
    protected AbstractApplicationContext appCtx;

    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.appCtx = (AbstractApplicationContext) applicationContext;
    }

    /**
     * @return the redisEndpoint
     */
    public String getEndpoint() {
        return this.f.getEndpoint();
    }

    /**
     * @param redisEndpoint
     *            the redisEndpoint to set, defaults to "redis://localhost:6379"
     */
    public void setEndpoint(String redisEndpoint) {
        this.f.setEndpoint(redisEndpoint);
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
        return this.discoveredUrisCount.get();
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
        //treat as disregarded
        appCtx.publishEvent(
            new CrawlURIDispositionEvent(this,curi,DISREGARDED));
        log(curi);
        incrementDisregardedUriCount();
        curi.stripToMinimal();
        curi.processingCleanup();
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
    	/*
        int allCount = allQueues.size();
        int inProcessCount = inProcessQueues.size();
        int readyCount = readyClassQueues.size();
        int snoozedCount = getSnoozedCount();
        int activeCount = inProcessCount + readyCount + snoozedCount;
        int inactiveCount = getTotalEligibleInactiveQueues();
        int ineligibleCount = getTotalIneligibleInactiveQueues();
        int retiredCount = getRetiredQueues().size();
        int exhaustedCount = allCount - activeCount - inactiveCount - retiredCount;
        */

        Map<String,Object> map = new LinkedHashMap<String, Object>();
        if( this.f.isConnected() ) {
	        map.put("totalQueues", this.f.getTotalQueues());
	        map.put("inProcessQueues", this.inFlight.get());
	        map.put("readyQueues", 0);
	        map.put("snoozedQueues", this.f.getScheduledQueues());
	        map.put("activeQueues", this.f.getActiveQueues());
	        map.put("inactiveQueues", 0);
	        map.put("ineligibleQueues", 0);
	        map.put("retiredQueues", this.f.getRetiredQueues());
	        map.put("exhaustedQueues", this.f.getExhaustedQueues());
	        map.put("lastReachedState", lastReachedState);
	        map.put("queueReadiedCount", queueReadiedCount.get());
        } else {
        	// This gets called after build, prior to launch, prior to start() (and therefore connect()):
	        map.put("totalQueues", 0);
	        map.put("inProcessQueues", this.inFlight.get());
	        map.put("readyQueues", 0);
	        map.put("snoozedQueues", 0);
	        map.put("activeQueues", 0);
	        map.put("inactiveQueues", 0);
	        map.put("ineligibleQueues", 0);
	        map.put("retiredQueues", 0);
	        map.put("exhaustedQueues", 0);
	        if( lastReachedState != null) {
		        map.put("lastReachedState", lastReachedState);
	        } else {
		        map.put("lastReachedState", "BUILT");
	        }
	        map.put("queueReadiedCount", queueReadiedCount.get());
        }
        return map;
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
    
    /** URIs scheduled to be re-enqueued at future date */
    protected SortedMap<Long, CrawlURI> futureUris = new ConcurrentSkipListMap<Long,CrawlURI>(); 
    
    /**
     * Check for any future-scheduled URIs now eligible for reenqueuing
     * (Copied from WorkQueueFrontier).
     * FIXME This should be in Redis too.
     * 
     */
    protected void checkFutures() {
//        assert Thread.currentThread() == managerThread;
        // TODO: consider only checking this every set interval
        if(!futureUris.isEmpty()) {
            synchronized(futureUris) {
                Iterator<CrawlURI> iter = 
                    futureUris.headMap(System.currentTimeMillis())
                        .values().iterator();
                while(iter.hasNext()) {
                    CrawlURI curi = iter.next();
                    curi.setRescheduleTime(-1); // unless again set elsewhere
                    iter.remove();
                    futureUriCount.decrementAndGet();
                    receive(curi);
                }
            }
        }
    }  

    /**
     * (Copied from WorkQueueFrontier):
     */
    public boolean isEmpty() {
        return queuedUriCount.get() == 0 
            && (uriUniqFilter == null || uriUniqFilter.pending() == 0)
            && futureUriCount.get() == 0;
    }
    

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
    	
        // consider rescheduled URIS
        checkFutures();

        // Get the next curi from the frontier:
        CrawlURI curi = this.f.next();

        // If there is one, return it:
        if (curi != null) {
        	this.inFlight.incrementAndGet();
            logger.finest("Returning: " + curi);
            // Ensure sheet overlays are applied (is appears these are not
            // persisted - the original BDB-based implementation does this too.)
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

    	logger.finer("Adding url "+curi + " to queue "+curi.getClassKey());
        boolean newUrl = this.f.enqueue(curi);
        if( newUrl ) {
        	logger.finer("Added NEW url "+curi + " to queue "+curi.getClassKey());
            this.discoveredUrisCount.incrementAndGet();
            this.incrementQueuedUriCount();
        }
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

        // Record that an in-flight CrawlURI has landed:
        this.inFlight.decrementAndGet();

        // FIXME this does not track and reset session/total budgets etc. Really
        // need to rank the 'available' queue to ensure hosts are not
        // re-activated quickly.
        //
        // wq.setSessionBudget(getBalanceReplenishAmount());
        // wq.setTotalBudget(getQueueTotalBudget());

        // codes/errors which don't consume the URI, leaving it atop queue
        if (needsReenqueuing(curi)) {
            logger.finest("Re-enqueing " + curi + " " + curi.getFetchStatus());
            if (curi.getFetchStatus() != S_DEFERRED) {
                // FIXME wq.expend(holderCost); // all retries but DEFERRED cost
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
        // Turns out the Frontier is also responsible for emitting the crawl log:
        log(curi);

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
            futureUris.put(curi.getRescheduleTime(),curi);
            futureUriCount.incrementAndGet();
        } else {
            curi.stripToMinimal();
            curi.processingCleanup();
        }

    }

    protected void delete(CrawlURI curi) {
    	logger.finer("Removing url "+curi + " from queue "+curi.getClassKey());
        this.f.dequeue(curi.getClassKey(), curi.getURI());
        this.decrementQueuedCount(1);
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
        logger.finest("Current inFlight = " + inFlight);
        return inFlight.get();
    }
    
    // heritrix_1  |   at org.archive.crawler.restlet.JobResource$2.write(JobResource.java:111)


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
        uriUniqFilter.setDestination(this);
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

    /**
     * FIXME This doesn't only delay the queue:
     * 
     * @param curi
     * @param fetchTime
     */
    private void setQueueDelay(CrawlURI curi, long fetchTime) {
        if (curi.includesRetireDirective()) {
            // Remove the queue from the fetch list:
            this.f.retireQueue(curi.getClassKey());
        } else {
            if (fetchTime == -1) {
                fetchTime = curi.getRescheduleTime();
            }
            this.incrementQueuedUriCount();
            this.f.reschedule(curi, fetchTime);
        }
    }

    @Override
    public long exportPendingUris(PrintWriter writer) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public ObjectIdentityCache<WorkQueue> getAllQueues() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public BlockingQueue<String> getReadyClassQueues() {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Set<WorkQueue> getInProcessQueues() {
        // TODO Auto-generated method stub
        return null;
    }

}
