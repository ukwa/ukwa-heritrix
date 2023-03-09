/**
 * 
 */
package uk.bl.wap.modules.extractor;

import java.util.Arrays;

import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ExtractorHTTP;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;

/**
 * @author anj
 *
 */
public class ExtractorHTTPWellKnownURIs extends ExtractorHTTP {

    private static final Logger LOGGER = Logger
            .getLogger(ExtractorHTTPWellKnownURIs.class.getName());

    // Default set of well-known paths:
    private String[] DEFAULT_WELL_KNOWN_PATHS = {
        "/humans.txt",
        "/ads.txt",
        "/sellers.json",
        "/.well-known/security.txt",
        "/.well-known/host-meta.json",
        "/.well-known/dat"
    };

    // Well-known paths that can be over-ridden at runtime:
    protected List<String> wellKnownPaths = Arrays.asList(DEFAULT_WELL_KNOWN_PATHS);

    public void setWellKnownPaths(List<String> wellKnownPaths) {
        this.wellKnownPaths = wellKnownPaths;
        LOGGER.fine("wellKnownPaths set to " + this.wellKnownPaths);
    }


	@Override
	protected void extract(CrawlURI curi) {
		// Do all the usual extractions:
		super.extract(curi);

		// Add some more well-known URIs:
        for( String wellKnownPath: wellKnownPaths) {
            addOutlink(curi, wellKnownPath, LinkContext.INFERRED_MISC, Hop.INFERRED);
        }
	}

}
