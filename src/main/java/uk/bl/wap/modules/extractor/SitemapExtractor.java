/**
 * 
 */
package uk.bl.wap.modules.extractor;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.io.IOUtils;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ContentExtractor;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;

import crawlercommons.sitemaps.AbstractSiteMap;
import crawlercommons.sitemaps.SiteMap;
import crawlercommons.sitemaps.SiteMapIndex;
import crawlercommons.sitemaps.SiteMapParser;
import crawlercommons.sitemaps.SiteMapURL;
import crawlercommons.sitemaps.UnknownFormatException;
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
            if (uri.is2XXSuccess()) {
                LOGGER.info("This url (" + uri
                        + ") is declared to be a sitemap (via robots.txt) and is a HTTP 200.");
                return true;
            } else {
                LOGGER.info("This url (" + uri
                        + ") is declared to be a sitemap (via robots.txt) but is a HTTP "
                        + uri.getFetchStatus() + ".");
            }
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
        AbstractSiteMap sitemap = parseSiteMap(uri);

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

    /**
     * Parse the sitemap using the Crawler Commons content-sniffing parser.
     * 
     * @param uri
     * @return
     */
    private AbstractSiteMap parseSiteMap(CrawlURI uri) {
        // The thing we will create:
        AbstractSiteMap sitemap = null;

        // Be strict about URLs but allow partial extraction:
        SiteMapParser smp = new SiteMapParser(true, true);
        // Parse it up:
        InputStream in;
        try {
            // Sitemaps are not supposed to be bigger than 50MB (according to
            // Google) so if we hit problems we can implement that limit:
            byte[] content = IOUtils.toByteArray(
                    uri.getRecorder().getContentReplayInputStream());
            if (content.length > 52428800) {
                LOGGER.warning("This sitemap is over 50MB " + uri + " "
                        + content.length);
            }
            // Now we can process it:
            sitemap = smp.parseSiteMap(content, new URL(uri.getURI()));
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,
                    "I/O Exception when parsing sitemap " + uri, e);
        } catch (UnknownFormatException e) {
            LOGGER.log(Level.WARNING,
                    "UnknownFormatException when parsing sitemap " + uri, e);
        }
        return sitemap;
    }

    private void recordOutlink(CrawlURI curi, URL newUri, Date lastModified,
            boolean isSitemap) {
        try {
            // Get the max outlinks (needed by add method):
            //
            // Because sitemaps are really important we excuse this extractor
            // from the general setting:
            //
            // getExtractorParameters().getMaxOutlinks();
            //
            // And instead use the maximum that is allowed for a sitemap:
            int max = 50000;

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
            // Count it:
            numberOfLinksExtracted.incrementAndGet();
        } catch (URIException e) {
            LOGGER.log(Level.WARNING,
                    "URIException when recording outlink " + newUri, e);
        }

    }

}
