package uk.bl.wap.modules.extractor;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class RobotsTxSitemapExtractorTest {
    public final static String ROBOTS_WITH_SITEMAP = "User-agent: *\nDisallow: /search/\nDisallow: /*.cfm$\nDisallow: /mps-lords-and-offices/mps/*?*\n\nSitemap: http://www.parliament.uk/sitemap.xml";
    public final static String ROBOTS_WITHOUT_SITEMAP = "User-agent: *\nDisallow: /search/\nDisallow: /*.cfm$\nDisallow: /mps-lords-and-offices/mps/*?*";

    @Test
    public void testSitemapExtraction() {
        RobotsTxtSitemapExtractor extractor = new RobotsTxtSitemapExtractor();
        InputStream input = new ByteArrayInputStream(
                ROBOTS_WITH_SITEMAP.getBytes());
        List<String> links = extractor.parseRobotsTxt(input);
        Assert.assertEquals(1, links.size());
        Assert.assertTrue(
                links.contains("http://www.parliament.uk/sitemap.xml"));
    }

    @Test
    public void testNonSitemapExtraction() {
        RobotsTxtSitemapExtractor extractor = new RobotsTxtSitemapExtractor();
        InputStream input = new ByteArrayInputStream(
                ROBOTS_WITHOUT_SITEMAP.getBytes());
        List<String> links = extractor.parseRobotsTxt(input);
        Assert.assertEquals(0, links.size());
    }
}
