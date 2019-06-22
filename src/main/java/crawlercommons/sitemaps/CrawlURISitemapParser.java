/**
 * 
 */
package crawlercommons.sitemaps;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.xml.sax.InputSource;

/**
 * 
 * FIXME Also support plain text sitemaps?
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class CrawlURISitemapParser {
    private static final Logger LOGGER = Logger
            .getLogger(CrawlURISitemapParser.class.getName());

    public static AbstractSiteMap getSitemap(CrawlURI uri) {
        AbstractSiteMap sitemap = null;
        SiteMapParser smp = new SiteMapParser();
        // Parse it up:
        InputStream in;
        try {
            in = new SkipLeadingWhiteSpaceInputStream(
                    (uri.getRecorder().getContentReplayInputStream()));
            InputSource is = new InputSource();
            is.setCharacterStream(
                    new BufferedReader(new InputStreamReader(in, UTF_8)));
            sitemap = smp.processXml(new URL(uri.getURI()), is);
        } catch (IOException e) {
            LOGGER.log(Level.WARNING,
                    "IO Exception when parsing sitemap " + uri, e);
        } catch (UnknownFormatException e) {
            LOGGER.log(Level.WARNING,
                    "UnknownFormatException when parsing sitemap " + uri, e);
        }
        return sitemap;
    }

}
