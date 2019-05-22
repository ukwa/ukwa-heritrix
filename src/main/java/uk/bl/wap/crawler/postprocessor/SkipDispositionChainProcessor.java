/**
 * 
 */
package uk.bl.wap.crawler.postprocessor;

import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.Processor;
import org.archive.modules.fetcher.FetchStatusCodes;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SkipDispositionChainProcessor extends Processor {

    private static final Logger logger = Logger
            .getLogger(SkipDispositionChainProcessor.class.getName());

    private static final Object SKIP_DISPOSITION_CHAIN = "skipDispositionChain";

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean shouldProcess(CrawlURI uri) {
        // Execute if the URL was ruled OOS by the
        // pre-selector in the Fetch Chain:
        if (uri.getFetchStatus() == FetchStatusCodes.S_OUT_OF_SCOPE) {
            return true;
        }

        // Check if the appropriate annotation is set:
        if (uri.getAnnotations().contains(SKIP_DISPOSITION_CHAIN)) {
            logger.finer("Found " + SKIP_DISPOSITION_CHAIN + " annotation for "
                    + uri);
            return true;
        }
        // Otherwise, default to skipping this Processor:
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#innerProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected void innerProcess(CrawlURI uri) throws InterruptedException {
        // Nothing to do here, see below...
    }

    /**
     * Here we override the ProcessResult to force the chain to FINISH:
     */
    @Override
    protected ProcessResult innerProcessResult(CrawlURI uri)
            throws InterruptedException {
        logger.info("Skipping remainder of disposition chain for " + uri
                + " status " + uri.getFetchStatus());
        return ProcessResult.jump("disposition");
    }


}
