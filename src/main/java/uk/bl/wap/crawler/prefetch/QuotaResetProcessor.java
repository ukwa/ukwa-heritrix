/**
 * 
 */
package uk.bl.wap.crawler.prefetch;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.crawler.framework.Frontier.FrontierGroup;
import org.archive.crawler.postprocessor.CandidatesProcessor;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.modules.fetcher.FetchStats;
import org.archive.modules.net.CrawlHost;
import org.archive.modules.net.CrawlServer;
import org.archive.modules.net.ServerCache;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
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
        clearQuotasFor(uri);
        // Also need to clear quotas for likely server aliases:
        // If http, do https:
        if (uri.getScheme() == "http") {
            try {
                String altUri = uri.getURI().replaceFirst("http", "https");
                UURI alt = UURIFactory.getInstance(altUri);
                clearQuotasFor(alt);
            } catch (URIException e) {
                logger.log(Level.WARNING,
                        "Could not generate https URI from http URI!", e);
            }
        }
        // If https, clear http:
        if (uri.getScheme() == "https") {
            try {
                String altUri = uri.getURI().replaceFirst("https", "http");
                UURI alt = UURIFactory.getInstance(altUri);
                clearQuotasFor(alt);
            } catch (URIException e) {
                logger.log(Level.WARNING,
                        "Could not generate https URI from http URI!", e);
            }
        }

        // And clear the FrontierGroup:
        // TODO NOTE that this does not clear the FrontierGroup for aliases.
        // TODO Perhaps quota resetting needs to be done differently, e.g.
        // inheriting the resetQuotas under certain circumstances (like
        // pre-requisite processing?)
        FrontierGroup group = candidates.getFrontier().getGroup(curi);
        synchronized (group) {
            if (group != null) {
                resetFetchStats(group.getSubstats(), "Frontier Group");
                group.makeDirty();
            }
        }

    }

    /**
     * Clear quotas associated with a particular URI:
     * 
     * @param uri
     */
    private void clearQuotasFor(UURI uri) {

        // By server:
        final CrawlServer server = serverCache.getServerFor(uri);
        if (server != null) {
            synchronized (server) {
                resetFetchStats(server.getSubstats(), "Server");
                server.makeDirty();
            }
        }

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
