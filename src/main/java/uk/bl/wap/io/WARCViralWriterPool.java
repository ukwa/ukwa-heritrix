package uk.bl.wap.io;

import java.util.concurrent.atomic.AtomicInteger;

import org.archive.io.WriterPool;
import org.archive.io.WriterPoolMember;
import org.archive.io.WriterPoolSettings;
import org.archive.io.warc.WARCWriterPoolSettings;

import uk.bl.wap.io.warc.WARCViralWriter;

/**
 * Minor variation of org.archive.modules.writer.WARCWriterPool to handle
 * conversion/viral records.
 * 
 * @author rcoram
 */

public class WARCViralWriterPool extends WriterPool {
	public WARCViralWriterPool( AtomicInteger serial, WriterPoolSettings settings, int poolMaximumActive, int poolMaximumWait ) {
		super( serial, settings, poolMaximumActive, poolMaximumWait );
	}

	@Override
	protected WriterPoolMember makeWriter() {
		return new WARCViralWriter( serialNo, ( WARCWriterPoolSettings ) settings );
	}
}
