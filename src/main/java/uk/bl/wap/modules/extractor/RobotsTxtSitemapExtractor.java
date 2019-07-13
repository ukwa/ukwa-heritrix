package uk.bl.wap.modules.extractor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ContentExtractor;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;

import uk.bl.wap.modules.deciderules.RecentlySeenDecideRule;

public class RobotsTxtSitemapExtractor extends ContentExtractor {
    private static final Logger LOGGER = Logger
            .getLogger(RobotsTxtSitemapExtractor.class.getName());
    private final static Pattern ROBOTS_PATTERN = Pattern
            .compile("^https?://[^/]+/robots.txt$");
    private final static Pattern SITEMAP_PATTERN = Pattern
            .compile("(?i)Sitemap:\\s*(.+)$");

    public final static String ANNOTATION_IS_SITEMAP = "isSitemap";

    @Override
    protected boolean shouldExtract(CrawlURI uri) {
        boolean shouldExtract = ROBOTS_PATTERN.matcher(uri.getURI()).matches();
        LOGGER.finest("Checked " + uri + " GOT " + shouldExtract);
        return shouldExtract;
    }

    public ArrayList<String> parseRobotsTxt(InputStream input) {
        ArrayList<String> links = new ArrayList<String>();
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        try {
            String line;
            Matcher matcher;
            while ((line = reader.readLine()) != null) {
                matcher = SITEMAP_PATTERN.matcher(line);
                if (matcher.matches()) {
                    links.add(matcher.group(1));
                }
            }
        } catch (IOException e) {
            LOGGER.warning(e.toString());
        }
        return links;
    }

    @Override
    protected boolean innerExtract(CrawlURI curi) {
        try {

            // Clone the CrawlURI and change hop path and avoid queueing
            // sitemaps as prerequisites (i.e. strip P from hop path).
            CrawlURI curiClone = new CrawlURI(curi.getUURI(),
                    curi.getPathFromSeed().replace("P", ""), curi.getVia(),
                    curi.getViaContext());

            // Also copy the source over, if one is set at all:
            if (curi.getSourceTag() != null) {
                // N.B. if the source tag gets explicitly set to null it all
                // goes a big bit wrong.
                curiClone.setSourceTag(curi.getSourceTag());
            }

            // Parse the robots for the sitemaps.
            List<String> links = parseRobotsTxt(
                    curi.getRecorder()
                    .getContentReplayInputStream());
            LOGGER.finest("Checked " + curi + " GOT " + links);

            // Get the max outlinks (needed by add method):
            int max = getExtractorParameters().getMaxOutlinks();

            // Accrue links:
            for (String link : links) {
                try {
                    // We've found a sitemap:
                    LOGGER.fine("Found site map: " + link);
                    numberOfLinksExtracted.incrementAndGet();

                    // Add links but using the cloned CrawlURI as the crawl
                    // context.
                    CrawlURI newCuri = addRelativeToBase(curiClone, max, link,
                            LinkContext.INFERRED_MISC, Hop.INFERRED);

                    // Annotate as a Site Map:
                    newCuri.getAnnotations().add(
                            RobotsTxtSitemapExtractor.ANNOTATION_IS_SITEMAP);

                    // Add immediate launchTimestamp to ensure re-crawl of the
                    // sitemap:
                    RecentlySeenDecideRule.addLaunchTimestamp(newCuri,
                            Calendar.getInstance().getTime());

                } catch (URIException e) {
                    logUriError(e, curi.getUURI(), link);
                }
            }

            // Patch outlinks back into original curi:
            for (CrawlURI outlink : curiClone.getOutLinks()) {
                curi.getOutLinks().add(outlink);
            }

            // Return number of links discovered:
            return (links.size() > 0);

        } catch (IOException e) {
            LOGGER.log(Level.WARNING, curi.getURI(), e);
            curi.getNonFatalFailures().add(e);
        }
        return false;
    }

}
