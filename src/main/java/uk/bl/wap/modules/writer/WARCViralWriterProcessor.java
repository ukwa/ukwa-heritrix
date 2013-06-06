package uk.bl.wap.modules.writer;

import static org.archive.io.warc.WARCConstants.HEADER_KEY_CONCURRENT_TO;
import static org.archive.io.warc.WARCConstants.HEADER_KEY_IP;
import static org.archive.io.warc.WARCConstants.HEADER_KEY_PAYLOAD_DIGEST;
import static org.archive.io.warc.WARCConstants.HTTP_REQUEST_MIMETYPE;
import static org.archive.io.warc.WARCConstants.HTTP_RESPONSE_MIMETYPE;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_FETCH_HISTORY;
import static org.archive.modules.recrawl.RecrawlAttributeConstants.A_WRITE_TAG;

import java.io.IOException;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.archive.io.ArchiveFileConstants;
import org.archive.io.ReplayInputStream;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.util.ArchiveUtils;
import org.archive.util.anvl.ANVLRecord;

import uk.bl.wap.io.WARCViralWriterPool;
import uk.bl.wap.io.warc.WARCViralWriter;

/**
 * Minor variation of org.archive.modules.writer.WARCWriterProcessor to handle
 * conversion/viral records.
 * 
 * @author rcoram
 */

public class WARCViralWriterProcessor extends org.archive.modules.writer.WARCWriterProcessor {
	private static final long serialVersionUID = 5818334757714399335L;
	private static final Logger logger = Logger.getLogger( WARCViralWriterProcessor.class.getName() );
	private AtomicLong urlsWritten = new AtomicLong();

	public WARCViralWriterProcessor() {

	}

	@Override
	protected void setupPool( final AtomicInteger serialNo ) {
		this.setPool( new WARCViralWriterPool( serialNo, this, getPoolMaxActive(), getMaxWaitForIdleMs() ) );
	}

	protected ProcessResult write( final String lowerCaseScheme, final CrawlURI curi ) throws IOException {
		WARCViralWriter writer = ( WARCViralWriter ) getPool().borrowFile();

		long position = writer.getPosition();
		try {
			// See if we need to open a new file because we've exceeded maxBytes.
			// Call to checkFileSize will open new file if we're at maximum for
			// current file.
			writer.checkSize();
			if( writer.getPosition() != position ) {
				// We just closed the file because it was larger than maxBytes.
				// Add to the totalBytesWritten the size of the first record
				// in the file, if any.
				setTotalBytesWritten( getTotalBytesWritten() + ( writer.getPosition() - position ) );
				position = writer.getPosition();
			}

			// Reset writer temp stats so they reflect only this set of records.
			// They'll be added to totals below, in finally block, after records
			// have been written.
			writer.resetTmpStats();
			// Write a request, response, and metadata all in the one
			// 'transaction'.
			final URI baseid = getRecordID();
			final String timestamp = ArchiveUtils.getLog14Date( curi.getFetchBeginTime() );
			if( lowerCaseScheme.startsWith( "http" ) ) {
				writeHttpRecords( curi, writer, baseid, timestamp );
			} else {
				logger.warning( "No handler for scheme " + lowerCaseScheme );
			}
		} catch( IOException e ) {
			// Invalidate this file (It gets a '.invalid' suffix).
			getPool().invalidateFile( writer );
			// Set the writer to null otherwise the pool accounting
			// of how many active writers gets skewed if we subsequently
			// do a returnWriter call on this object in the finally block.
			writer = null;
			throw e;
		} finally {
			if( writer != null ) {
				if( WARCViralWriter.getStat( writer.getTmpStats(), WARCViralWriter.TOTALS, WARCViralWriter.NUM_RECORDS ) > 0l ) {
					addStats( writer.getTmpStats() );
					urlsWritten.incrementAndGet();
				}
				if( logger.isLoggable( Level.FINE ) ) {
					logger.fine( "wrote " + WARCViralWriter.getStat( writer.getTmpStats(), WARCViralWriter.TOTALS, WARCViralWriter.SIZE_ON_DISK ) + " bytes to " + writer.getFile().getName() + " for " + curi );
				}
				setTotalBytesWritten( getTotalBytesWritten() + ( writer.getPosition() - position ) );
				getPool().returnFile( writer );

				String filename = writer.getFile().getName();
				if( filename.endsWith( ArchiveFileConstants.OCCUPIED_SUFFIX ) ) {
					filename = filename.substring( 0, filename.length() - ArchiveFileConstants.OCCUPIED_SUFFIX.length() );
				}
				curi.addExtraInfo( "warcFilename", filename );

				@SuppressWarnings( "unchecked" )
				Map<String, Object>[] history = ( Map<String, Object>[] ) curi.getData().get( A_FETCH_HISTORY );
				if( history != null && history[ 0 ] != null ) {
					history[ 0 ].put( A_WRITE_TAG, filename );
				}
			}
		}
		return checkBytesWritten();
	}

	private void writeHttpRecords( final CrawlURI curi, WARCViralWriter w, final URI baseid, final String timestamp ) throws IOException {
		// Add named fields for ip, checksum, and relate the metadata
		// and request to the resource field.
		// TODO: Use other than ANVL (or rename ANVL as NameValue or
		// use RFC822 (commons-httpclient?).
		ANVLRecord headers = new ANVLRecord( 5 );
		if( curi.getContentDigest() != null ) {
			headers.addLabelValue( HEADER_KEY_PAYLOAD_DIGEST, curi.getContentDigestSchemeString() );
		}
		headers.addLabelValue( HEADER_KEY_IP, getHostAddress( curi ) );
		URI rid;

		rid = writeResponse( w, timestamp, HTTP_RESPONSE_MIMETYPE, baseid, curi, headers );

		headers = new ANVLRecord( 1 );
		headers.addLabelValue( HEADER_KEY_CONCURRENT_TO, '<' + rid.toString() + '>' );

		if( getWriteRequests() ) {
			writeRequest( w, timestamp, HTTP_REQUEST_MIMETYPE, baseid, curi, headers );
		}
		if( getWriteMetadata() ) {
			writeMetadata( w, timestamp, baseid, curi, headers );
		}
	}

	protected URI writeResponse( final WARCViralWriter w, final String timestamp, final String mimetype, final URI baseid, final CrawlURI curi, final ANVLRecord namedFields ) throws IOException {
		ReplayInputStream ris = curi.getRecorder().getRecordedInput().getReplayInputStream();
		try {
			w.writeResponseRecord( curi.toString(), timestamp, mimetype, baseid, namedFields, ris, curi.getRecorder().getRecordedInput().getSize() );
		} finally {
			IOUtils.closeQuietly( ris );
		}
		return baseid;
	}
}