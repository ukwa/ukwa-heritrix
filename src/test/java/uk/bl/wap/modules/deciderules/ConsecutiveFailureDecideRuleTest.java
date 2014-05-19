package uk.bl.wap.modules.deciderules;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.extractor.LinkContext;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.junit.Assert;
import org.junit.Test;

public class ConsecutiveFailureDecideRuleTest {

    @Test
    public void testSuccessSuccess() throws Exception {
	ConsecutiveFailureDecideRule dr = new ConsecutiveFailureDecideRule();
	CrawlURI uri = createTestUri("http://www.bl.uk/robots.txt", 200);
	CrawlURI referrer = createTestUri("http://www.bl.uk/", 200);
	uri.setFullVia(referrer);

	Assert.assertEquals(DecideResult.NONE, dr.decisionFor(uri));
    }

    @Test
    public void testSuccessFailure() throws Exception {
	ConsecutiveFailureDecideRule dr = new ConsecutiveFailureDecideRule();
	CrawlURI uri = createTestUri("http://www.bl.uk/robots.txt", 404);
	CrawlURI referrer = createTestUri("http://www.bl.uk/", 200);
	uri.setFullVia(referrer);

	Assert.assertEquals(DecideResult.NONE, dr.decisionFor(uri));
    }

    @Test
    public void testFailureFailure() throws Exception {
	ConsecutiveFailureDecideRule dr = new ConsecutiveFailureDecideRule();
	CrawlURI uri = createTestUri("http://www.bl.uk/robots.txt", 404);
	CrawlURI referrer = createTestUri("http://www.bl.uk/", 404);
	uri.setFullVia(referrer);

	Assert.assertEquals(DecideResult.REJECT, dr.decisionFor(uri));
    }

    @Test
    public void testFailureSuccess() throws Exception {
	ConsecutiveFailureDecideRule dr = new ConsecutiveFailureDecideRule();
	CrawlURI uri = createTestUri("http://www.bl.uk/robots.txt", 200);
	CrawlURI referrer = createTestUri("http://www.bl.uk/", 404);
	uri.setFullVia(referrer);

	Assert.assertEquals(DecideResult.NONE, dr.decisionFor(uri));
    }

    private CrawlURI createTestUri(String urlStr, int statusCode) throws URIException {
	UURI testUuri = UURIFactory.getInstance(urlStr);
	CrawlURI testUri = new CrawlURI(testUuri, null, null,
		LinkContext.NAVLINK_MISC);
	testUri.setFetchStatus(statusCode);
	return testUri;
    }
}
