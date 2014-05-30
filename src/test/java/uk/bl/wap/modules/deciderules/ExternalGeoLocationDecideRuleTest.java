package uk.bl.wap.modules.deciderules;

import java.io.File;
import java.io.FileOutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.zip.GZIPInputStream;

import junit.framework.Assert;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.extractor.LinkContext;
import org.archive.modules.fetcher.DefaultServerCache;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.junit.Before;
import org.junit.Test;

public class ExternalGeoLocationDecideRuleTest {
    public static final String GEOLITE_CITY = "http://geolite.maxmind.com/download/geoip/database/GeoLite2-City.mmdb.gz";
    private ExternalGeoLookup lookup = new ExternalGeoLookup();

    @Before
    public void setup() throws Exception {
	File temp = File.createTempFile("act", ".tmp", new File("./"));
	temp.deleteOnExit();
	URL geo = new URL(GEOLITE_CITY);
	HttpURLConnection connection = (HttpURLConnection) geo.openConnection();
	GZIPInputStream input = new GZIPInputStream(connection.getInputStream());
	ReadableByteChannel channel = Channels.newChannel(input);
	FileOutputStream output = new FileOutputStream(temp);
	output.getChannel().transferFrom(channel, 0, Long.MAX_VALUE);
	output.close();
	lookup.setDatabase(temp.getAbsolutePath());
    }

    @Test
    public void testCountryCodeInList() throws Exception {
	List<String> cc = new ArrayList<String>();
	cc.add(Locale.UK.getCountry());
	ExternalGeoLocationDecideRule dr = makeDecideRule(cc);
	dr.setDecision(DecideResult.ACCEPT);
	CrawlURI testUri = createTestUri("http://www.bl.uk/");

	Assert.assertEquals(DecideResult.ACCEPT, dr.decisionFor(testUri));
    }

    @Test
    public void testCountryCodeNotInList() throws Exception {
	List<String> cc = new ArrayList<String>();
	cc.add(Locale.FRANCE.getCountry());
	ExternalGeoLocationDecideRule dr = makeDecideRule(cc);
	dr.setDecision(DecideResult.ACCEPT);
	CrawlURI testUri = createTestUri("http://www.bl.uk/");

	Assert.assertEquals(DecideResult.NONE, dr.decisionFor(testUri));
    }

    @Test
    public void testEmptyList() throws Exception {
	List<String> cc = new ArrayList<String>();
	ExternalGeoLocationDecideRule dr = makeDecideRule(cc);
	dr.setDecision(DecideResult.ACCEPT);
	CrawlURI testUri = createTestUri("http://www.bl.uk/");

	Assert.assertEquals(DecideResult.NONE, dr.decisionFor(testUri));
    }

    private CrawlURI createTestUri(String urlStr) throws URIException {
	UURI testUuri = UURIFactory.getInstance(urlStr);
	CrawlURI testUri = new CrawlURI(testUuri, null, null,
		LinkContext.NAVLINK_MISC);

	return testUri;
    }

    private ExternalGeoLocationDecideRule makeDecideRule(
	    List<String> countryCodes) {
	ExternalGeoLocationDecideRule e = new ExternalGeoLocationDecideRule();
	e.setLookup(this.lookup);
	e.setCountryCodes(countryCodes);
	e.setServerCache(new DefaultServerCache());
	return e;
    }
}
