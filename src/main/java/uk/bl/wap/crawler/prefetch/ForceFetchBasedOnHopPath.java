/**
 * 
 */
package uk.bl.wap.crawler.prefetch;

import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ForceFetchBasedOnHopPath extends Processor {

    /** */
    private int forceFetchHopDepth = 1;

    /** */
    private boolean navlinksOnly = true;

    /**
     * @return the forceFetchHopDepth
     */
    public int getForceFetchHopDepth() {
        return forceFetchHopDepth;
    }

    /**
     * @param forceFetchHopDepth
     *            the forceFetchHopDepth to use, defaults to 1
     */
    public void setForceFetchHopDepth(int forceFetchHopDepth) {
        this.forceFetchHopDepth = forceFetchHopDepth;
    }

    /**
     * @return the navlinksOnly
     */
    public boolean isNavlinksOnly() {
        return navlinksOnly;
    }

    /**
     * @param navlinksOnly
     *            Only count navigational links when assesing the depth.
     */
    public void setNavlinksOnly(boolean navlinksOnly) {
        this.navlinksOnly = navlinksOnly;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean shouldProcess(CrawlURI uri) {

        // Only count navigational links ('L'):
        if (this.navlinksOnly) {
            if (uri.getLinkHopCount() < this.forceFetchHopDepth) {
                return true;
            }
        }

        // Or, count the whole hope path:
        else {
            if (uri.getHopCount() < this.forceFetchHopDepth) {
                return true;
            }
        }

        // Otherwise:
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#innerProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected void innerProcess(CrawlURI uri) throws InterruptedException {
        // Set forceFetch so the URL will be crawled even if it's already been
        // seen.
        uri.setForceFetch(true);
    }

}
