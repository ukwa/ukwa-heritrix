package uk.bl.wap.modules.seeds;

import java.io.File;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;

import org.archive.modules.CrawlURI;
import org.archive.modules.SchedulingConstants;
import org.archive.modules.seeds.SeedModule;
import org.archive.net.UURIFactory;

import uk.bl.wap.net.SOAPUtil;

/**
 * Module that announces a configurable number of seeds from an
 * external webservice.
 * 
 * @author rcoram
 */

public class DbSeedModule extends SeedModule {
	private static final Logger LOGGER = Logger.getLogger( DbSeedModule.class.getName() );
	private static final long serialVersionUID = -7673364036049809858L;
	
	private String webServiceUrl;
	private String imaxResults;
	private String crawlerId;
	private String crawlerPoolSize;
	
	public void setWebServiceUrl( String url ) {
		this.webServiceUrl = url;
	}
	
	public void setImaxResults( String max ) {
		this.imaxResults = max;
	}
	
	public void setCrawlerId( String id ) {
		this.crawlerId = id;
	}
	
	public void setCrawlerPoolSize( String pool ) {
		this.crawlerPoolSize = pool;
	}

	@Override
	public void actOn( File file ) {
	}

	@Override
	public void addSeed( CrawlURI curi ) {
		publishAddedSeed( curi );
	}

	@Override
	public void announceSeeds() {
		ArrayList<String> seeds = new ArrayList<String>();
		try {
			seeds = SOAPUtil.getUncrawledSeeds( this.imaxResults, this.crawlerId, this.crawlerPoolSize, this.webServiceUrl );
			Iterator<String> iterator = seeds.iterator();
			while( iterator.hasNext() ) {
				CrawlURI curi = new CrawlURI( UURIFactory.getInstance( iterator.next() ) );
				curi.setSeed( true );
				curi.setSchedulingDirective( SchedulingConstants.MEDIUM );
	            if( getSourceTagSeeds() ) {
	            	curi.setSourceTag(curi.toString());
	            }
				publishAddedSeed( curi );
			}
		} catch( URIException e ) {
			LOGGER.log( Level.SEVERE, "announceSeeds: " + e.toString() );
		}
	}
}
