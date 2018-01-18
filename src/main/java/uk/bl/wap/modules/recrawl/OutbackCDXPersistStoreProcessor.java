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
            .getLogger(OutbackCDXPersistLoadProcessor.class.getName());

    @Override
    protected ProcessResult innerProcessResult(CrawlURI curi)
            throws InterruptedException {

        //
        // Update the status CDX when something important happens.
        //
        // 1. Do not push this record if a different engine is handling this
        // (-5002:BLOCKED-BY-CUSTOM-PROCESSOR e.g. web rendering). If we don't
        // do this, we can collide on timestamp(seconds)+URL.
        //
        // 2. Also, do not push this even to the CDX server if we are just
        // awaiting a prerequisite (-50:DEFERRED).
        //

        if (curi.getFetchStatus() != FetchStatusCodes.S_BLOCKED_BY_CUSTOM_PROCESSOR
                && curi.getFetchStatus() != FetchStatusCodes.S_DEFERRED) {
            this.outbackCDXClient.putUri(curi);
        }

        return ProcessResult.PROCEED;
    }

}
