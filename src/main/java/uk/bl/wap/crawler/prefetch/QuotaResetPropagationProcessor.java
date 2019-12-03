/**
 * 
 */
package uk.bl.wap.crawler.prefetch;

import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.modules.extractor.Hop;

/**
 * 
 * This ensures any quota reset annotation is propagated to pre-requisites or
 * redirects. i.e. if a seed should have it's quota reset, then any direct
 * redirects should do, and it should also be done before getting robots.txt to
 * ensure the quota's don't prevent prequisites being fetched.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class QuotaResetPropagationProcessor extends Processor {

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean shouldProcess(CrawlURI uri) {
        // Does the URL we are one hop from have a quota reset annotation?
        if (uri.getFullVia() != null
                && uri.getFullVia().getAnnotations() != null
                && uri.getFullVia().getAnnotations()
                        .contains(QuotaResetProcessor.RESET_QUOTAS)) {
            // AND
            // Is this new URI a pre-requisite or a redirect?
            if (uri.getLastHop().equals(Hop.PREREQ.getHopString())
                    || uri.getLastHop().equals(Hop.REFER.getHopString())) {
                // Then process this item to add the annotation:
                return true;
            }
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#innerProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected void innerProcess(CrawlURI uri) throws InterruptedException {
        // Add the annotation:
        if (!uri.getAnnotations().contains(QuotaResetProcessor.RESET_QUOTAS)) {
            uri.getAnnotations().add(QuotaResetProcessor.RESET_QUOTAS);
        }
    }

}
