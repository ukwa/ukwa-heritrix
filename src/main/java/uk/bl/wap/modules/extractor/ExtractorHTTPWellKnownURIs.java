/**
 * 
 */
package uk.bl.wap.modules.extractor;

import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ExtractorHTTP;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;

/**
 * @author anj
 *
 */
public class ExtractorHTTPWellKnownURIs extends ExtractorHTTP {

	@Override
	protected void extract(CrawlURI curi) {
		// Do all the usual extractions:
		super.extract(curi);
		// Add some more well-known URIs:
        addOutlink(curi, "/humans.txt", LinkContext.INFERRED_MISC, Hop.INFERRED);
        addOutlink(curi, "/ads.txt", LinkContext.INFERRED_MISC, Hop.INFERRED);
        addOutlink(curi, "/sellers.json", LinkContext.INFERRED_MISC, Hop.INFERRED);
        addOutlink(curi, "/security.txt", LinkContext.INFERRED_MISC, Hop.INFERRED);
        addOutlink(curi, "/.well-known/host-meta.json", LinkContext.INFERRED_MISC, Hop.INFERRED);
        addOutlink(curi, "/.well-known/dat", LinkContext.INFERRED_MISC, Hop.INFERRED);
	}

}
