package uk.bl.wap.io.warc;

import org.archive.io.ArchiveFileConstants;

/**
 * Minor variation of org.archive.modules.writer.WARCConstants to define additional
 * information for conversion/viral records.
 * @author rcoram
 */

public interface WARCConstants extends ArchiveFileConstants {
	public static final String HEADER_KEY_REFERS_TO = "WARC-Refers-To";
	public static final String HEADER_KEY_BLOCK_DIGEST = "WARC-Block-Digest";
	public static final String HEADER_KEY_PAYLOAD_TYPE = "WARC-Identified-Payload-Type";
	
	public static final String VIRAL_CONTENT_MIMETYPE = "application/http; encoding=bytewise_xor_with_118";
}
