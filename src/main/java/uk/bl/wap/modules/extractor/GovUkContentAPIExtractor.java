/**
 * 
 */
package uk.bl.wap.modules.extractor;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.Extractor;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;

/**
 * 
 * This module implements the logic for guessing a Content API URL for GOV.UK
 * content
 * 
 * See https://content-api.publishing.service.gov.uk/
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class GovUkContentAPIExtractor extends Extractor {
    private static final Logger LOGGER = Logger
            .getLogger(GovUkContentAPIExtractor.class.getName());

    /* (non-Javadoc)
     * @see org.archive.modules.extractor.Extractor#extract(org.archive.modules.CrawlURI)
     */
    @Override
    protected void extract(CrawlURI curi) {
        // Take the URI and make a new one with /api/content/ inserted at
        // the start of the path.
        String path;
        try {
            path = "/api/content" + curi.getUURI().getPath();
            // And add it to the discovered links:
            addOutlink(curi, path, LinkContext.INFERRED_MISC, Hop.INFERRED);
            numberOfLinksExtracted.incrementAndGet();

        } catch (URIException e) {
            LOGGER.log(Level.WARNING, "URI exception when handling " + curi, e);
        }

    }

    /* (non-Javadoc)
     * @see org.archive.modules.Processor#shouldProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean shouldProcess(CrawlURI uri) {
        // If must be on gov.uk:
        if (!uri.getURI().startsWith("https://www.gov.uk/")) {
            return false;
        }
        // There must be a content type:
        if (uri.getContentType() == null) {
            return false;
        }
        // If must be HTML or XHTML:
        if (!uri.getContentType().contains("text/html")
                && !uri.getContentType().contains("application/xml+xhtml")) {
            return false;
        }
        // All tests passed:
        return true;
    }

}
