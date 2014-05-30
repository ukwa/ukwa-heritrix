package uk.bl.wap.crawler.processor;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.modules.net.CrawlHost;
import org.archive.modules.net.ServerCache;
import org.springframework.beans.factory.annotation.Autowired;

public class CountryCodeAnnotator extends Processor {
    private final static Logger LOGGER = Logger
	    .getLogger(CountryCodeAnnotator.class.getName());
    protected ServerCache serverCache;

    @Autowired
    public void setServerCache(ServerCache serverCache) {
	this.serverCache = serverCache;
    }

    @Override
    protected boolean shouldProcess(CrawlURI uri) {
	return true;
    }

    @Override
    protected void innerProcess(CrawlURI curi) {
	try {
	    CrawlHost host = serverCache.getHostFor(curi.getUURI());
	    if (host != null) {
		String countryCode = host.getCountryCode();
		if (countryCode != null && !countryCode.equals("")
			&& !countryCode.equals("--")) {
		    curi.getAnnotations().add("geo:" + countryCode);
		}
	    }
	} catch (Exception e) {
	    LOGGER.log(Level.WARNING,
		    "Problem adding CountryCode: " + curi.getURI(), e);
	    curi.getNonFatalFailures().add(e);
	}
    }

}
