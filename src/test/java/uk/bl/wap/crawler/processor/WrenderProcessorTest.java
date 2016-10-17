package uk.bl.wap.crawler.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;

import org.archive.modules.CrawlURI;
import org.archive.net.UURIFactory;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

/**
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WrenderProcessorTest {

    /**
     * 
     * @throws JSONException
     * @throws IOException
     */
    @Test
    public void testProcessHar() throws JSONException, IOException {
        // String wrenderUrl =
        // "http://127.0.0.1:8000/render?url=http://example.org/&warc_prefix=wrender";
        File wrjson = new File("src/test/resources/wrender-example.json");
        WrenderProcessor wp = new WrenderProcessor();
        CrawlURI curi = new CrawlURI(
                UURIFactory.getInstance("http://example.org/"));
        // wp.process(curi);
        JSONObject har = WrenderProcessor
                .readJsonFromUrl(wrjson.toURI().toString());
        WrenderProcessor.processHar(har, curi);

        assertEquals("Status code was not found!", 200, curi.getFetchStatus());
        assertTrue("Annotation was not found!",
                curi.getAnnotations().contains("Wrendered"));
        assertEquals("CrawlURIs not as expected!", 1,
                curi.getOutLinks().size());
        CrawlURI l = curi.getOutLinks().iterator().next();
        assertEquals("Can't find expected outlink!",
                "http://www.iana.org/domains/example", l.getURI());

    }

}
