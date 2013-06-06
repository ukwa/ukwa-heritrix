package uk.bl.wap.io.warc;

import static uk.bl.wap.io.warc.WARCConstants.VIRAL_CONTENT_MIMETYPE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.concurrent.atomic.AtomicInteger;

import org.archive.io.warc.WARCWriter;
import org.archive.io.warc.WARCWriterPoolSettings;
import org.archive.util.anvl.ANVLRecord;

import uk.bl.wap.io.XorInputStream;

/**
 * @author rcoram
 */

public class WARCViralWriter extends WARCWriter {
	public WARCViralWriter( AtomicInteger serialNo, WARCWriterPoolSettings settings ) {
		super( serialNo, settings );
	}

	@Override
	public void writeResponseRecord( final String url, final String create14DigitDate, final String mimetype, final URI recordId, final ANVLRecord namedFields, InputStream response, final long responseLength ) throws IOException {
		writeRecord( CONVERSION, url, create14DigitDate, VIRAL_CONTENT_MIMETYPE, recordId, namedFields, new XorInputStream( response ), responseLength, true );
	}
}
