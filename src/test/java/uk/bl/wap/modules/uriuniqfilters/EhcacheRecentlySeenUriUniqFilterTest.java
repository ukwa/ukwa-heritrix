/**
 * 
 */
package uk.bl.wap.modules.uriuniqfilters;

import static org.junit.Assert.assertEquals;

import java.io.File;

import org.apache.commons.httpclient.URIException;
import org.archive.crawler.datamodel.UriUniqFilter;
import org.archive.modules.CrawlURI;
import org.archive.net.UURIFactory;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class EhcacheRecentlySeenUriUniqFilterTest {

    String surtFile = "src/test/resources/surt-to-ttl.txt";

    /**
     * Little URL reciever for testing.
     *
     */
    public static class Receiver implements UriUniqFilter.CrawlUriReceiver {
        boolean received = false;

        @Override
        public void receive(CrawlURI item) {
            this.received = true;
        }
    }

    private EhcacheRecentlySeenUriUniqFilter uuf;

    /**
     * @throws java.lang.Exception
     */
    @Before
    public void setUp() throws Exception {
        // Logging:
        System.setProperty("java.util.logging.config.file",
                new File("src/test/resources/logging.properties")
                        .getAbsolutePath());

        // Set up the filter
        uuf = new EhcacheRecentlySeenUriUniqFilter();
        uuf.setRecentlySeenTTLsecs(10);
        uuf.start();
    }

    /**
     * 
     * @param uuf
     * @param key
     * @param reciptExpected
     * @throws URIException
     */
    private void checkFilter(RecentlySeenUriUniqFilter uuf, String key,
            boolean reciptExpected) throws URIException {

        // Setup
        Receiver rx = new Receiver();
        uuf.setDestination(rx);

        // Add
        CrawlURI curi = new CrawlURI(UURIFactory.getInstance(key));
        uuf.add(key, curi);

        // And check:
        assertEquals("Cache misfire!", reciptExpected, rx.received);

    }

    /**
     * 
     * @throws URIException
     * @throws InterruptedException
     */
    @Test
    public void testCache() throws URIException, InterruptedException {
        // Add and check:
        checkFilter(uuf, "http://www.bbc.co.uk", true);
        checkFilter(uuf, "http://www.bbc.co.uk", false);
        checkFilter(uuf, "http://www.bbc.com", true);
        checkFilter(uuf, "http://www.bbc.com", false);
        // Wait for the system to forget:
        Thread.sleep(2 * 1000);
        // And re-try:
        checkFilter(uuf, "http://www.bbc.co.uk", true);
        // But this still hasn't timed-out:
        checkFilter(uuf, "http://www.bbc.com", false);
    }

}
