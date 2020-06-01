/**
 * 
 */
package uk.bl.wap.modules.recrawl;

import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.fetcher.FetchStatusCodes;

/**
 * 
 * This extends the PersistLoadProcessor and puts the current crawl URI into
 * OutbackCDX
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXPersistStoreProcessor
        extends OutbackCDXPersistLoadProcessor {

    private static final Logger logger = Logger
            .getLogger(OutbackCDXPersistStoreProcessor.class.getName());

    @Override
    protected ProcessResult innerProcessResult(CrawlURI curi)
            throws InterruptedException {

        //
        // Update the status CDX when something important happens: i.e. events
        // that we should treat as meaning 'this URL has been dealt with for
        // now'.
        //
        // 1. Do not push this record if a different engine is handling this
        // (-5002:BLOCKED-BY-CUSTOM-PROCESSOR e.g. web rendering). If we don't
        // do this, we can collide on timestamp(seconds)+URL.
        //
        // 2. Also, do not push this even to the CDX server if we are just
        // awaiting a prerequisite (-50:DEFERRED).
        //
        // 3. Do not push this record if the condition is something we wish to
        // treat as a transient within the overall recrawl time, for example we
        // will allow a retry later if it's hit a quota. Similarly, if the URL
        // gets dropped from scope e.g. by the recentlySeen decide rule.
        //
        // Any status that would get re-enqueued by the Frontier should not get
        // logged in OutbackCDX or the 'recently seen' logic will prevent
        // retries. See the states that result in true from:
        //
        // org.archive.crawler.frontier.AbstractFrontier.needsReenqueuing(CrawlURI)
        //

        if (curi.getFetchStatus() != FetchStatusCodes.S_BLOCKED_BY_CUSTOM_PROCESSOR
                && curi.getFetchStatus() != FetchStatusCodes.S_DEFERRED
                && curi.getFetchStatus() != FetchStatusCodes.S_OUT_OF_SCOPE
                && curi.getFetchStatus() != FetchStatusCodes.S_BLOCKED_BY_QUOTA
                && curi.getFetchStatus() != FetchStatusCodes.S_CONNECT_FAILED
                && curi.getFetchStatus() != FetchStatusCodes.S_CONNECT_LOST) {
            this.outbackCDXClient.putUri(curi);
        }

        return ProcessResult.PROCEED;
    }

}
