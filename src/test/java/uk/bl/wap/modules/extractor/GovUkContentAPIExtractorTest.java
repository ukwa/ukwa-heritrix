/**
 * 
 */
package uk.bl.wap.modules.extractor;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.LinkContext;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class GovUkContentAPIExtractorTest {

    @Test
    public void testShouldProcess() throws URIException {
        GovUkContentAPIExtractor e = new GovUkContentAPIExtractor();
        // Out of scope:
        CrawlURI curi = createTestUri("http://www.bl.uk/");
        curi.setContentType("text/html");
        Assert.assertFalse(e.shouldProcess(curi));

        // Not html but in scope:
        Assert.assertFalse(e.shouldProcess(createExpectedUri()));

        // Good to go:
        Assert.assertTrue(e.shouldProcess(createTestUri()));
    }

    @Test
    public void testShouldExtract() throws URIException, InterruptedException {
        GovUkContentAPIExtractor e = new GovUkContentAPIExtractor();

        // Run it:
        CrawlURI curi = createTestUri();
        e.process(curi);

        // Check for the expected outlink:
        boolean found = false;
        CrawlURI expected = createExpectedUri();
        for (CrawlURI l : curi.getOutLinks()) {
            if (l.equals(expected)) {
                found = true;
            }
        }
        Assert.assertTrue(found);

    }

    private CrawlURI createTestUri() throws URIException {
        CrawlURI curi = createTestUri("https://www.gov.uk/take-pet-abroad");
        curi.setContentType("text/html");
        return curi;
    }

    /*
     * Set up the expected outlink, with the hop etc.
     */
    private CrawlURI createExpectedUri() throws URIException {
        UURI testUuri = UURIFactory
                .getInstance("https://www.gov.uk/api/content/take-pet-abroad");
        CrawlURI testUri = new CrawlURI(testUuri, "I",
                createTestUri().getUURI(),
                LinkContext.INFERRED_MISC);
        testUri.setContentType("application/json");

        return testUri;
    }

    private CrawlURI createTestUri(String urlStr) throws URIException {
        UURI testUuri = UURIFactory.getInstance(urlStr);
        CrawlURI testUri = new CrawlURI(testUuri, null, null,
                LinkContext.NAVLINK_MISC);

        return testUri;
    }
}
