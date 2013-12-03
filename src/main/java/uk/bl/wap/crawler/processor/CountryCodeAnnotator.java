package uk.bl.wap.crawler.processor;

import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.modules.net.CrawlHost;
import org.archive.modules.net.ServerCache;

public class CountryCodeAnnotator extends Processor {
	private final static Logger LOGGER = Logger.getLogger( CountryCodeAnnotator.class.getName() );
	protected ServerCache serverCache;

	@Override
	protected boolean shouldProcess( CrawlURI uri ) {
		return true;
	}

	@Override
	protected void innerProcess( CrawlURI curi ) {
		try {
			CrawlHost host = serverCache.getHostFor( curi.getUURI() );
			String countryCode = host.getCountryCode();
			if( countryCode != null && !countryCode.equals( "" ) && !countryCode.equals( "--" ) ) {
				curi.getAnnotations().add( "countryCode:" + countryCode );
			}
		} catch( Exception e ) {
			LOGGER.warning( e.getMessage() );
		}
	}

}
