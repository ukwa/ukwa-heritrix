/**
 * 
 */
package uk.bl.wap.util;

import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;

import org.junit.Test;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXClientTest {

    /**
     * Test method for
     * {@link uk.bl.wap.util.OutbackCDXClient#getLastCrawl(java.lang.String)}.
     * 
     * @throws URISyntaxException
     * @throws InterruptedException
     * @throws IOException
     */
    @Test
    public void testCheckValid()
            throws IOException, InterruptedException, URISyntaxException {
        OutbackCDXClient ocdx = new OutbackCDXClient();

        String[] qurls = {
                "httpss:uk.linkedin.com/pub/michael-lenny/10/a32/a30/",
                "http://allprintjerseyyourlocalembroideryandvinylprintspecialisthomepage/",
                "http://development-social-marketing-strategy-promote-ebola-treatment-seeking-behaviour-sierra-leone/" };

        for (String qurl : qurls) {
            HashMap<String, Object> result = ocdx.getLastCrawl(qurl);
            assertNull(result);
        }
    }

}
