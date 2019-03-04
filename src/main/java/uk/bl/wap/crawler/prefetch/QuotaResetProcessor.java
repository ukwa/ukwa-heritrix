/**
 * 
 */
package uk.bl.wap.crawler.prefetch;

import java.util.logging.Logger;

import org.archive.crawler.framework.Frontier.FrontierGroup;
import org.archive.crawler.postprocessor.CandidatesProcessor;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.modules.fetcher.FetchStats;
import org.archive.modules.net.CrawlHost;
import org.archive.modules.net.CrawlServer;
import org.archive.modules.net.ServerCache;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class QuotaResetProcessor extends Processor {

    public static final String RESET_QUOTAS = "resetQuotas";

    private static final Logger logger = Logger
            .getLogger(QuotaResetProcessor.class.getName());

    protected CandidatesProcessor candidates;

    public CandidatesProcessor getCandidates() {
        return candidates;
    }

    @Autowired
    public void setCandidates(CandidatesProcessor candidates) {
        this.candidates = candidates;
    }

    protected ServerCache serverCache;

    public ServerCache getServerCache() {
        return this.serverCache;
    }

    @Autowired
    public void setServerCache(ServerCache serverCache) {
        this.serverCache = serverCache;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        // Check if the appropriate KEY is set:
        if (curi.getData().containsKey(RESET_QUOTAS)) {
            logger.finer("Found reset-quota annotation for " + curi);
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#innerProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        logger.finer("Clearing down quota stats for " + curi);

        // Group stats:
        FrontierGroup group = candidates.getFrontier().getGroup(curi);
        synchronized (group) {
            if (group != null) {
                resetFetchStats(group.getSubstats(), "Frontier Group");
                group.makeDirty();
            }
        }
        // By server:
        final CrawlServer server = serverCache.getServerFor(curi.getUURI());
        if (server != null) {
            synchronized (server) {
                resetFetchStats(server.getSubstats(), "Server");
                server.makeDirty();
            }
        }
        // And by host:
        final CrawlHost host = serverCache.getHostFor(curi.getUURI());
        // Host can be null if lookup fails:
        if (host != null) {
            synchronized (host) {
                resetFetchStats(host.getSubstats(), "Host");
                host.makeDirty();
            }
        }
    }

    /*
     * java.lang.NullPointerException: ACK at
     * org.archive.spring.KeyedProperties.get(KeyedProperties.java:65) at
     * org.archive.modules.deciderules.DecideRule.getEnabled(DecideRule.java:38)
     * at
     * org.archive.modules.deciderules.DecideRule.decisionFor(DecideRule.java:
     * 57) at
     * org.archive.modules.deciderules.DecideRule.accepts(DecideRule.java:77) at
     * org.archive.crawler.spring.SheetOverlaysManager.applyOverlaysTo(
     * SheetOverlaysManager.java:329) at
     * org.archive.crawler.postprocessor.CandidatesProcessor.runCandidateChain(
     * CandidatesProcessor.java:164) at
     * uk.bl.wap.crawler.frontier.KafkaUrlReceiver$CrawlMessageFrontierScheduler
     * .run(KafkaUrlReceiver.java:544) at
     * java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java
     * :1149) at
     * java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.
     * java:624) at java.lang.Thread.run(Thread.java:748)
     * 
     * 
     */

    private void resetFetchStats(FetchStats fs, String kind) {
        // Record initial state:
        String before = fs.shortReportLine();

        // Resets all tallies that can be used by QuotaEnforcer:
        fs.put(FetchStats.FETCH_SUCCESSES, 0L);
        fs.put(FetchStats.SUCCESS_BYTES, 0L);
        fs.put(FetchStats.FETCH_RESPONSES, 0L);
        fs.put(FetchStats.TOTAL_BYTES, 0L);
        fs.put(FetchStats.NOVEL, 0L);
        fs.put(FetchStats.NOVELCOUNT, 0L);

        // Report the result of the resetting:
        logger.fine("Reset " + kind + " stats from " + before + " to "
                + fs.shortReportLine());
    }

}
