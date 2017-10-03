/**
 * 
 */
package uk.bl.wap.modules.recrawl;

import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;

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

        // Only +ve FetchStatusCodes values belong in the persist store:
        if (curi.getFetchStatus() > 0) {
            this.outbackCDXClient.putUri(curi);
        }
        return ProcessResult.PROCEED;
    }

}
