package uk.bl.wap.modules.deciderules;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.LinkContext;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.junit.Assert;
import org.junit.Test;

public class CompressibilityDecideRuleTest {
    public final String HIGHLY_COMPRESSIBLE_URL = "http://brereton.org.uk/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton/brinton.css";
    public final String HIGHLY_INCOMPRESSIBLE_URL = "http://4c1j5b2p0cv4w1x8rx2y39umgw5q85s7.uk/0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
    public final String STANDARD_URL = "http://www.bl.uk/";
    public final double MIN = 0.28D;
    public final double MAX = 1.6D;

    @Test
    public void testMinRange() throws Exception {
	CompressibilityDecideRule dr = makeDecideRule(MIN, Double.MAX_VALUE);
	CrawlURI testUri = createTestUri(HIGHLY_COMPRESSIBLE_URL);

	Assert.assertTrue(dr.evaluate(testUri));
    }

    @Test
    public void testMidRange() throws Exception {
	CompressibilityDecideRule dr = makeDecideRule(MIN, MAX);
	CrawlURI testUri = createTestUri("http://www.archive.org");

	Assert.assertFalse(dr.evaluate(testUri));
    }

    @Test
    public void testMaxRange() throws Exception {
	CompressibilityDecideRule dr = makeDecideRule(0D, MAX);
	CrawlURI testUri = createTestUri("http://www.archive.org");

	Assert.assertTrue(dr.evaluate(testUri));
    }

    private CrawlURI createTestUri(String urlStr) throws URIException {
	UURI testUuri = UURIFactory.getInstance(urlStr);
	CrawlURI testUri = new CrawlURI(testUuri, null, null,
		LinkContext.NAVLINK_MISC);

	return testUri;
    }

    private CompressibilityDecideRule makeDecideRule(double min, double max) {
	CompressibilityDecideRule c = new CompressibilityDecideRule();
	c.setMin(min);
	c.setMax(max);
	return c;
    }

    public static void main(String[] args) throws Exception {
	CompressibilityDecideRuleTest c = new CompressibilityDecideRuleTest();
	c.testMinRange();
	c.testMidRange();
	c.testMaxRange();
    }
}