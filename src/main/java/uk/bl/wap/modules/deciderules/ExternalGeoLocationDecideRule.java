package uk.bl.wap.modules.deciderules;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.DecideResult;
import org.archive.modules.deciderules.ExternalGeoLookupInterface;
import org.archive.modules.net.CrawlHost;
import org.xbill.DNS.Address;

public class ExternalGeoLocationDecideRule extends
	org.archive.modules.deciderules.ExternalGeoLocationDecideRule {
    private static final long serialVersionUID = 8790007524831875385L;

    private static final Logger LOGGER = Logger
	    .getLogger(ExternalGeoLocationDecideRule.class.getName());
    protected List<String> countryCodes = new ArrayList<String>();
    protected ExternalGeoLookupInterface lookup = null;

    @Override
    public void setCountryCodes(List<String> codes) {
	this.countryCodes = codes;
	LOGGER.fine("countryCodes set to " + this.countryCodes);
    }

    @Override
    public DecideResult onlyDecision(CrawlURI uri) {
	return this.getDecision();
    }

    @Override
    protected boolean evaluate(CrawlURI curi) {
	ExternalGeoLookupInterface impl = getLookup();
	if (impl == null) {
	    return false;
	}
	CrawlHost crawlHost = null;
	String host;
	InetAddress address;
	try {
	    host = curi.getUURI().getHost();
	    crawlHost = serverCache.getHostFor(host);
	    if (crawlHost == null) {
		return false;
	    }
	    if (crawlHost.getCountryCode() != null) {
		return countryCodes.contains(crawlHost.getCountryCode());
	    }
	    address = crawlHost.getIP();
	    if (address == null) {
		address = Address.getByName(host);
	    }
	    String cc = (String) impl.lookup(address);
	    if (cc != null) {
		crawlHost.setCountryCode(cc);
		if (countryCodes.contains(crawlHost.getCountryCode())) {
		    LOGGER.fine("Country Code Lookup: " + host + " "
			    + crawlHost.getCountryCode());
		    return true;
		}
	    }
	} catch (UnknownHostException e) {
	    LOGGER.log(Level.FINE, "Failed dns lookup " + curi, e);
	    curi.getNonFatalFailures().add(e);
	    if (crawlHost != null) {
		crawlHost.setCountryCode("--");
	    }
	} catch (URIException e) {
	    LOGGER.log(Level.FINE, "Failed to parse hostname " + curi, e);
	    curi.getNonFatalFailures().add(e);
	}
	return false;
    }
}
