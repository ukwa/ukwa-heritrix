package uk.bl.wap.modules.extractor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.extractor.LinkContext;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ExtractorJsonTest {
    public final String jsonNullValues = "{\"error\":{\"code\":-32700,\"message\":\"Parse Error.\",\"name\":\"JSONRPCError\"},\"id\":null,\"result\":null}";
    public final String jsonFullUriValue = "{\"error\":{\"code\":-32700,\"message\":\"Parse Error.\",\"name\":\"JSONRPCError\",\"url\":\"http://www.bl.uk/\"},\"id\":null,\"result\":null}";
    public final String jsonRelativeUriValue = "{\"error\":{\"code\":-32700,\"message\":\"Parse Error.\",\"name\":\"JSONRPCError\",\"path\":\"/path/to/some/page/\"},\"id\":null,\"result\":null}";

    @Test
    public void testShouldProcessJsonContent() throws Exception {
	ExtractorJson e = new ExtractorJson();
	CrawlURI curi = createTestUri("http://www.bl.uk/");
	curi.setContentType("application/json");

	Assert.assertTrue(e.shouldExtract(curi));
    }

    @Test
    public void testShouldNotProcessNonJsonContent() throws Exception {
	ExtractorJson e = new ExtractorJson();
	CrawlURI curi = createTestUri("http://www.bl.uk/");
	curi.setContentType("application/html");

	Assert.assertFalse(e.shouldExtract(curi));
    }

    @Test
    public void testShouldProcessJsonUri() throws Exception {
	ExtractorJson e = new ExtractorJson();
	CrawlURI curi = createTestUri("http://www.bl.uk/url.json");

	Assert.assertTrue(e.shouldExtract(curi));
    }

    @Test
    public void testShouldNotProcessNonJsonUri() throws Exception {
	ExtractorJson e = new ExtractorJson();
	CrawlURI curi = createTestUri("http://www.bl.uk/");

	Assert.assertFalse(e.shouldExtract(curi));
    }

    @Test
    public void testNullValues() throws Exception {
	ExtractorJson e = new ExtractorJson();
	List<String> links = new ArrayList<String>();
	e.parse(getRootNode(jsonNullValues), links);

	Assert.assertEquals(0, links.size());
    }

    @Test
    public void testFullUriValues() throws Exception {
	ExtractorJson e = new ExtractorJson();
	List<String> links = new ArrayList<String>();
	e.parse(getRootNode(jsonFullUriValue), links);

	Assert.assertEquals(1, links.size());
    }

    @Test
    public void testRelativeUriValues() throws Exception {
	ExtractorJson e = new ExtractorJson();
	List<String> links = new ArrayList<String>();
	e.parse(getRootNode(jsonRelativeUriValue), links);

	Assert.assertEquals(1, links.size());
    }

    private JsonNode getRootNode(String json) throws JsonProcessingException,
	    IOException {
	JsonFactory factory = new JsonFactory();
	ObjectMapper mapper = new ObjectMapper(factory);
	return mapper.readTree(json);
    }

    private CrawlURI createTestUri(String urlStr) throws URIException {
	UURI testUuri = UURIFactory.getInstance(urlStr);
	CrawlURI testUri = new CrawlURI(testUuri, null, null,
		LinkContext.NAVLINK_MISC);

	return testUri;
    }
}
