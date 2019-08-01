/**
 * 
 */
package uk.bl.wap.crawler.prefetch;

import java.util.logging.Logger;

import org.archive.crawler.postprocessor.CandidatesProcessor;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.modules.fetcher.FetchStats;
import org.archive.modules.net.CrawlHost;
import org.archive.modules.net.CrawlServer;
import org.archive.modules.net.ServerCache;
import org.archive.net.UURI;
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
        // Check if the appropriate annotation is set:
        if (curi.getAnnotations().contains(RESET_QUOTAS)) {
            logger.finer("Found reset-quota annotation for " + curi);
            return true;
        }
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.archive.modules.Processor#innerProcess(org.archive.modules.CrawlURI)
     * 
     */
    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        logger.info("Clearing quotas for " + curi);

        // Clear the quota for this URI
        UURI uri = curi.getUURI();

        // By host:
        final CrawlHost host = serverCache.getHostFor(uri);
        // Host can be null if lookup fails:
        if (host != null) {
            synchronized (host) {
                resetFetchStats(host.getSubstats(), "Host");
                host.makeDirty();
            }
        }

        // By server ('authority' i.e. including port):
        final CrawlServer server = serverCache.getServerFor(uri);
        if (server != null) {
            synchronized (server) {
                resetFetchStats(server.getSubstats(), "Server");
                server.makeDirty();
            }
        }


        // NOTE Does not modify FrontierGroup FetchStats as this appears to
        // destablize the frontier. Lots of "Can not set
        // org.archive.modules.fetcher.FetchStats field
        // org.archive.crawler.frontier.WorkQueue.substats to java.lang.Byte"
        // ADDEDUM: Even after removing this, these errors still turned up.
        // Now wondering if attempting to clear http and https stats here was
        // causing the problem somehow.

    }

    /**
     * 
     * Reset all substats for a particular kind:
     * 
     */
    private void resetFetchStats(FetchStats fs, String kind) {
        // Record initial state:
        String before = fs.shortReportLine();

        // Resets all tallies that can be used by QuotaEnforcer:
        fs.put(FetchStats.FETCH_SUCCESSES, 0L);
        fs.put(FetchStats.FETCH_RESPONSES, 0L);
        fs.put(FetchStats.SUCCESS_BYTES, 0L);
        fs.put(FetchStats.FETCH_RESPONSES, 0L);
        fs.put(FetchStats.TOTAL_BYTES, 0L);
        fs.put(FetchStats.NOVEL, 0L);
        fs.put(FetchStats.NOVELCOUNT, 0L);


        // Report the result of the resetting:
        logger.finer("Reset " + kind + " stats from " + before + " to "
                + fs.shortReportLine());
    }

}
