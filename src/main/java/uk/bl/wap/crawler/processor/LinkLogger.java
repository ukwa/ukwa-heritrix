package uk.bl.wap.crawler.processor;

import java.lang.StringBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.archive.crawler.frontier.BdbFrontier;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.util.ArchiveUtils;
import org.archive.util.MimetypeUtils;
import org.archive.modules.CoreAttributeConstants;
import org.springframework.beans.factory.annotation.Autowired;

import uk.bl.wap.net.SOAPUtil;

/**
 * Logs URI to the Link Repository
 * 
 * 
 * 
 * @author rcoram
 */

public class LinkLogger extends Processor {
	private static final long serialVersionUID = 882486980784429178L;
	private static final Logger LOGGER = Logger.getLogger( LinkLogger.class.getName() );
	private static final String delimiter = "||||";
	private Vector<String> logRecords = new Vector<String>();
	private int recordsLogged = 0;

	protected BdbFrontier frontier;

	@Autowired
	public void setFrontier( BdbFrontier frontier ) {
		this.frontier = frontier;
	}

	@Override
	protected void innerProcess( CrawlURI curi ) throws InterruptedException {
		try {
			logRecords.add( this.format( curi ) );
			if( this.logRecords.size() >= Integer.parseInt( kp.get( "numBatchRecords" ).toString() ) || this.frontier.queuedUriCount() <= 1 ) {
				int entries = this.logRecords.size();

				ArrayList<String> workingCopy = this.getWorkingCopy();

				int result = this.postSoap( workingCopy );
				recordsLogged += result;
				if( result != entries ) {
					throw new Exception( "Logged " + result + " , expected " + entries );
				}
			}
		} catch( Exception e ) {
			LOGGER.log( Level.SEVERE, "innerProcess: " + e );
		}
	}
	
	private synchronized ArrayList<String> getWorkingCopy() {
		ArrayList<String> workingCopy = new ArrayList<String>( this.logRecords );
		this.logRecords.clear();
		return workingCopy;
	}

	@Override
	protected boolean shouldProcess( CrawlURI uri ) {
		return true;
	}

	public String getWebServiceUrl() {
		return ( String ) kp.get( "webServiceUrl" );
	}

	public void setWebServiceUrl( String url ) {
		kp.put( "webServiceUrl", url );
	}

	public int getNumBatchRecords() {
		return ( int ) Integer.parseInt( kp.get( "numBatchRecords" ).toString() );
	}

	public void setNumBatchRecords( int numRecords ) {
		kp.put( "numBatchRecords", numRecords );
	}

	public int getCrawlerId() {
		return ( int ) Integer.parseInt( kp.get( "crawlerId" ).toString() );
	}

	public void setCrawlerId( int crawlerId ) {
		kp.put( "crawlerId", crawlerId );
	}

	private int postSoap( ArrayList<String> links ) {
		int numRecords = 0;
		try {
			StringBuffer buffer = new StringBuffer();
			Iterator<String> iterator = links.iterator();
			if( iterator.hasNext() )
				buffer.append( iterator.next() );
			while( iterator.hasNext() ) {
				buffer.append( delimiter );
				buffer.append( iterator.next() );
			}
			numRecords = SOAPUtil.LogBatchPIPE( buffer.toString(), Integer.toString( this.getCrawlerId() ), this.getWebServiceUrl() );
		} catch( NumberFormatException n ) {
			LOGGER.log( Level.SEVERE, "Invalid SOAP response: " + n.getMessage() );
		} catch( Exception e ) {
			LOGGER.log( Level.SEVERE, "postSoap: " + e );
		}
		return numRecords;
	}

	/**
	 * Taken from org.archive.crawler.io.UriProcessingFormatter as we need a slightly different output.
	 * 
	 * @param curi
	 * @return
	 */
	private String format( CrawlURI curi ) {
		String length = "-";
		String mime = null;
		String retries = "-";
		String ip = "-";
		if( curi.isHttpTransaction() ) {
			if( curi.getContentLength() >= 0 ) {
				length = Long.toString( curi.getContentLength() );
			} else if( curi.getContentSize() > 0 ) {
				length = Long.toString( curi.getContentSize() );
			}
			mime = curi.getContentType();
		} else {
			if( curi.getContentSize() > 0 ) {
				length = Long.toString( curi.getContentSize() );
			}
			mime = curi.getContentType();
		}
		mime = MimetypeUtils.truncate( mime );

		long time = System.currentTimeMillis();
		String arcTimeAndDuration;
		if( curi.containsDataKey( CoreAttributeConstants.A_FETCH_COMPLETED_TIME ) ) {
			long completedTime = curi.getFetchCompletedTime();
			long beganTime = curi.getFetchBeginTime();
			arcTimeAndDuration = ArchiveUtils.get17DigitDate( beganTime ) + "+" + Long.toString( completedTime - beganTime );
		} else {
			arcTimeAndDuration = "-";
		}

		String via = curi.flattenVia();
		String digest = curi.getContentDigestSchemeString();

		String sourceTag = curi.containsDataKey( CoreAttributeConstants.A_SOURCE_TAG ) ? curi.getSourceTag() : null;

		StringBuffer buffer = new StringBuffer();
		buffer.append( ArchiveUtils.getLog17Date( time ) ).append( " " ).append( curi.getFetchStatus() ).append( " " ).append( length ).append( " " ).append( curi.getUURI().toString() ).append( " " ).append( checkForNull( curi.getPathFromSeed() ) ).append( " " ).append( checkForNull( via ) ).append( " " ).append( mime ).append( " " ).append( "#" ).append( Integer.toString( curi.getThreadNumber() ) ).append( " " ).append( arcTimeAndDuration ).append( " " ).append( checkForNull( digest ) ).append( " " ).append( checkForNull( sourceTag ) ).append( " " );
		Collection<String> collection = curi.getAnnotations();
		Iterator<String> iterator = collection.iterator();
		while( iterator.hasNext() ) {
			String annotation = iterator.next();
			if( annotation.matches( "[0-9]+t" ) )
				retries = annotation;
			if( annotation.matches( "[0-9]+\\.[0-9]+\\.[0-9]+\\.[0-9]+" ) )
				ip = annotation;
		}
		buffer.append( retries ).append( " " ).append( ip );
		return buffer.toString();
	}

	protected String checkForNull( String str ) {
		return ( str == null || str.length() <= 0 ) ? "-" : str;
	}

	@Override
	public String report() {
		StringBuffer report = new StringBuffer();
		report.append( super.report() );
		report.append( "  Links processed: " + this.getURICount() + "\n" );
		report.append( "  Links logged:    " + this.recordsLogged + "\n" );

		return report.toString();
	}
}