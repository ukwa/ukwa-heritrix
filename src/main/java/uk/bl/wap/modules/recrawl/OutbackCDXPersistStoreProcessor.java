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

        // Do not push this record if a different engine (e.g. Wrender) is
        // handling this (otherwise we can collide on timestamp(seconds)+URL:
        if (curi.getFetchStatus() != FetchStatusCodes.S_BLOCKED_BY_CUSTOM_PROCESSOR) {
            this.outbackCDXClient.putUri(curi);
        }

        return ProcessResult.PROCEED;
    }

}
