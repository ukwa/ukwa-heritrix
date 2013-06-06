package uk.bl.wap.crawler.processor;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.springframework.beans.factory.annotation.Autowired;

import uk.bl.wap.util.ClamdScanner;

/**
 * 
 * @author rcoram
 */

public class ViralContentProcessor extends Processor {
	private final static Logger LOGGER = Logger.getLogger( ViralContentProcessor.class.getName() );
	private static final long serialVersionUID = -321505737175991914L;
	private int virusCount = 0;

	public ViralContentProcessor() {}

	/**
	 * The host machine on which clamd is running.
	 */
	@Autowired
	public void setClamdHost( String clamdHost ) {
		kp.put( "clamdHost", clamdHost );
	}

	private String getClamdHost() {
		return ( String ) kp.get( "clamdHost" );
	}

	/**
	 * The port on which the instance of clamd can be found.
	 */
	public int getClamdPort() {
		return ( Integer ) kp.get( "clamdPort" );
	}

	@Autowired
	public void setClamdPort( int port ) {
		kp.put( "clamdPort", port );
	}

	/**
	 * The timeout in milliseconds for clamd.
	 */
	@Autowired
	public void setClamdTimeout( int clamdTimeout ) {
		kp.put( "clamdTimeout", clamdTimeout );
	}

	private int getClamdTimeout() {
		return ( Integer ) kp.get( "clamdTimeout" );
	}

	private ClamdScanner createScanner() {
		return new ClamdScanner( this.getClamdHost(), this.getClamdPort(), this.getClamdTimeout() );
	}

	@Override
	protected void innerProcess( CrawlURI curi ) throws InterruptedException {
		try {
			ClamdScanner scanner = createScanner();
			String result = scanner.clamdScan( curi.getRecorder().getReplayInputStream() );
			if( result.matches( "^([1-2]:\\s+)?stream:.+$" ) ) {
				if( !result.matches( "^([1-2]:\\s+)?stream: OK.*$" ) ) {
					curi.getAnnotations().add( result );
					virusCount++;
				}
			} else {
				LOGGER.log( Level.WARNING, "Invalid ClamAV response: " + result );
			}
		} catch( Exception e ) {
			LOGGER.log( Level.WARNING, "innerProcess(): " + e.toString() );
		}
	}

	@Override
	protected boolean shouldProcess( CrawlURI uri ) {
		return uri.is2XXSuccess() && ( uri.getContentLength() > 0L );
	}

	@Override
	public String report() {
		StringBuffer report = new StringBuffer();
		report.append( super.report() );
		report.append( "  Streams scanned: " + this.getURICount() + "\n" );
		report.append( "  Viruses found:   " + this.virusCount + "\n" );

		return report.toString();
	}
}