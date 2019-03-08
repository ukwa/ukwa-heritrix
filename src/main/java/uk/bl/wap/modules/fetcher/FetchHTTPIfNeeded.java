/**
 * 
 */
package uk.bl.wap.modules.fetcher;

import org.archive.modules.CrawlURI;
import org.archive.modules.fetcher.FetchHTTP;

/**
 * 
 * This is a very small modification to FetchHTTP which checks if an earlier
 * processor has already downloaded the item.
 * 
 * This makes it easier to 'stack' different HTTP downloaders.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class FetchHTTPIfNeeded extends FetchHTTP {

    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        // Only attempt if no fetch has been attempted yet:
        if (curi.getFetchStatus() != 0) {
            return false;
        }
        return super.shouldProcess(curi);
    }

}
