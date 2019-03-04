/**
 * 
 */
package uk.bl.wap.crawler.postprocessor;

import java.util.logging.Logger;

import org.archive.crawler.datamodel.UriUniqFilter;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ForgettingFrontierProcessor extends Processor {

    private static final Logger logger = Logger
            .getLogger(ForgettingFrontierProcessor.class.getName());

    protected UriUniqFilter uriUniqFilter;

    public UriUniqFilter getUriUniqFilter() {
        return this.uriUniqFilter;
    }
    @Autowired
    public void setUriUniqFilter(UriUniqFilter uriUniqFilter) {
        this.uriUniqFilter = uriUniqFilter;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        return true;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#innerProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        logger.finer("Making the frontier forget it has seen " + curi);
        uriUniqFilter.forget(curi.getCanonicalString(), curi);
    }

}
