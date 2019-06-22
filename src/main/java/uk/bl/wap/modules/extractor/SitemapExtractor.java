/**
 * 
 */
package uk.bl.wap.modules.extractor;

import java.net.URL;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ContentExtractor;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;

import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.CrawlURISitemapParser;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapURL;
import uk.bl.wap.modules.deciderules.RecentlySeenDecideRule;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class SitemapExtractor extends ContentExtractor {
    private static final Logger LOGGER = Logger
            .getLogger(SitemapExtractor.class.getName());

    /* (non-Javadoc)
     * @see org.archive.modules.extractor.ContentExtractor#shouldExtract(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean shouldExtract(CrawlURI uri) {
        // If declared as such:
        if (uri.getAnnotations()
                .contains(RobotsTxtSitemapExtractor.ANNOTATION_IS_SITEMAP)) {
            LOGGER.info("This is declared to be a sitemap (via robots.txt): "
                    + uri);
            return true;
        }

        // Via content type:
        String mimeType = uri.getContentType();
        if (mimeType != null ) {
            // Looks like XML:
            if (mimeType.toLowerCase().startsWith("text/xml")
                    || mimeType.toLowerCase().startsWith("application/xml")) {

                // check if content starts with xml preamble "<?xml" and does
                // contain "<urlset " or "<sitemapindex" early in the content
                String contentStartingChunk = uri.getRecorder()
                        .getContentReplayPrefixString(400);
                if (contentStartingChunk.matches("(?is)[\\ufeff]?<\\?xml\\s.*")
                        && contentStartingChunk.matches(
                                "(?is).*(?:<urlset|<sitemapindex[>\\s]).*")) {
                    LOGGER.info("Based on content sniffing, this is a sitemap: "
                            + uri);
                    return true;
                }
            }
        }
        
        // Otherwise, not
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.modules.extractor.ContentExtractor#innerExtract(org.archive.modules.CrawlURI)
     */
    @Override
    protected boolean innerExtract(CrawlURI uri) {
        // Parse the sitemap:
        AbstractSiteMap sitemap = CrawlURISitemapParser.getSitemap(uri);

        // Did that work?
        if (sitemap != null) {
            // Process results:
            if (sitemap.isIndex()) {
                final Collection<AbstractSiteMap> links = ((SiteMapIndex) sitemap)
                        .getSitemaps();
                for (final AbstractSiteMap asm : links) {
                    if (asm == null) {
                        continue;
                    }
                    this.recordOutlink(uri, asm.getUrl(), asm.getLastModified(),
                            true);
                }
            } else {
                final Collection<SiteMapURL> links = ((SiteMap) sitemap)
                        .getSiteMapUrls();
                for (final SiteMapURL url : links) {
                    if (url == null) {
                        continue;
                    }
                    this.recordOutlink(uri, url.getUrl(), url.getLastModified(),
                            false);
                }
            }
        }

        return false;
    }

    private void recordOutlink(CrawlURI curi, URL newUri, Date lastModified,
            boolean isSitemap) {
        try {
            // Get the max outlinks (needed by add method):
            int max = getExtractorParameters().getMaxOutlinks();
            // Add the URI:
            CrawlURI newCuri = addRelativeToBase(curi, max, newUri.toString(),
                    LinkContext.INFERRED_MISC, Hop.INFERRED);
            // Annotate it, if it's a sitemap:
            if (isSitemap) {
                newCuri.getAnnotations()
                        .add(RobotsTxtSitemapExtractor.ANNOTATION_IS_SITEMAP);
                // Add immediate launchTimestamp to ensure sub-sitemaps are
                // recrawled by default (unless overridden below):
                RecentlySeenDecideRule.addLaunchTimestamp(newCuri,
                        Calendar.getInstance().getTime());
            }
            // Use the date if set:
            if( lastModified != null) {
                RecentlySeenDecideRule.addLaunchTimestamp(newCuri,
                        lastModified);
            }
            // And log about it:
            LOGGER.fine("Found " + newUri + " from " + curi + " Dated "
                    + lastModified + " and with isSitemap = " + isSitemap);
        } catch (URIException e) {
            LOGGER.log(Level.WARNING,
                    "URIException when recording outlink " + newUri, e);
        }

    }

}
