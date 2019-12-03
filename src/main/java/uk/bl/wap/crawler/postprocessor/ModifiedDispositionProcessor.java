/**
 * 
 */
package uk.bl.wap.crawler.postprocessor;

import static org.archive.modules.fetcher.FetchStatusCodes.S_CONNECT_FAILED;
import static org.archive.modules.fetcher.FetchStatusCodes.S_CONNECT_LOST;
import static org.archive.modules.fetcher.FetchStatusCodes.S_DEEMED_NOT_FOUND;
import static org.archive.modules.fetcher.FetchStatusCodes.S_DEFERRED;
import static org.archive.modules.fetcher.FetchStatusCodes.S_OUT_OF_SCOPE;
import static org.archive.modules.fetcher.FetchStatusCodes.S_BLOCKED_BY_CUSTOM_PROCESSOR;

import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.crawler.postprocessor.DispositionProcessor;
import org.archive.modules.CrawlURI;
import org.archive.modules.net.CrawlServer;
import org.archive.modules.net.IgnoreRobotsPolicy;

/**
 * 
 * This takes the original disposition processor and modifies it to cope with
 * the case where robots.txt is not re-crawled because it's been seen too
 * recently.
 * 
 * Also modifies politenessDelay handling, so slow WebRender events do not lead
 * to long post-WebRender delays.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ModifiedDispositionProcessor extends DispositionProcessor {

    private static final Logger logger = Logger
            .getLogger(ModifiedDispositionProcessor.class.getName());
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.archive.crawler.postprocessor.DispositionProcessor#innerProcess(org.
     * archive.modules.CrawlURI)
     */
    @Override
    protected void innerProcess(CrawlURI curi) {
        // Tally per-server, per-host, per-frontier-class running totals
        CrawlServer server = serverCache.getServerFor(curi.getUURI());

        String scheme = curi.getUURI().getScheme().toLowerCase();
        if (scheme.equals("http") || scheme.equals("https") && server != null) {
            // Update connection problems counter
            if (curi.getFetchStatus() == S_CONNECT_FAILED
                    || curi.getFetchStatus() == S_CONNECT_LOST) {
                server.incrementConsecutiveConnectionErrors();
            } else if (curi.getFetchStatus() > 0) {
                server.resetConsecutiveConnectionErrors();
            }

            // Update robots info ANJ: UNLESS IT GOT KNOCKED OUT OF SCOPE (i.e.
            // Recently Seen so won't be fetched unless it's an true
            // pre-requisite)
            try {
                if ("/robots.txt".equals(curi.getUURI().getPath())
                        && curi.getFetchStatus() != S_DEFERRED
                        && curi.getFetchStatus() != S_OUT_OF_SCOPE) { // <<<
                    // shortcut retries w/ DEEMED when ignore-all
                    if (metadata
                            .getRobotsPolicy() instanceof IgnoreRobotsPolicy) {
                        if (curi.getFetchStatus() < 0
                                && curi.getFetchStatus() != S_DEFERRED) {
                            // prevent the rest of the usual retries
                            curi.setFetchStatus(S_DEEMED_NOT_FOUND);
                        }
                    }

                    // Update server with robots info
                    // NOTE: in some cases the curi's status can be changed here
                    server.updateRobots(curi);
                }
            } catch (URIException e) {
                logger.severe("Failed get path on " + curi.getUURI());
            }
        }

        // set politeness delay
        // ANJ: but only if one hasn't been set elsewhere
        if (curi.getFetchStatus() == S_BLOCKED_BY_CUSTOM_PROCESSOR) {
            curi.setPolitenessDelay(getMinDelayMs());
        } else {
            curi.setPolitenessDelay(politenessDelayFor(curi));
        }

        // consider operator-set force-retire
        if (getForceRetire()) {
            curi.setForceRetire(true);
        }

        // TODO: set other disposition decisions
        // success, failure, retry(retry-delay)
    }

}
