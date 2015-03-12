package uk.bl.wap.modules.extractor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.ContentExtractor;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;

public class RobotsTxtSitemapExtractor extends ContentExtractor {
    private static final Logger LOGGER = Logger
            .getLogger(RobotsTxtSitemapExtractor.class.getName());
    public final static Pattern ROBOTS_PATTERN = Pattern
            .compile("^https?://[^/]+/robots.txt$");
    public final static Pattern SITEMAP_PATTERN = Pattern
            .compile("(?i)Sitemap:\\s*(.+)$");

    @Override
    protected boolean shouldExtract(CrawlURI uri) {
        return ROBOTS_PATTERN.matcher(uri.getURI()).matches();
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
            List<String> links = parseRobotsTxt(curi.getRecorder()
                    .getContentReplayInputStream());
            for (String link : links) {
                curi.createCrawlURI(link, LinkContext.SPECULATIVE_MISC,
                        Hop.SPECULATIVE);
            }
        } catch (IOException e) {
            LOGGER.log(Level.WARNING, curi.getURI(), e);
            curi.getNonFatalFailures().add(e);
        }
        return false;
    }

}
