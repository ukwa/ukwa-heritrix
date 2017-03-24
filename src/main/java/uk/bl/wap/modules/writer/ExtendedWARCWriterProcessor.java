/**
 * 
 */
package uk.bl.wap.modules.writer;

import org.archive.io.warc.WARCWriter;
import org.archive.modules.CrawlURI;
import org.archive.modules.writer.WARCWriterProcessor;

/**
 * 
 * This just extends the WARC Writer to store the (compressed) WARC Record
 * length.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ExtendedWARCWriterProcessor extends WARCWriterProcessor {

    @Override
    protected void updateMetadataAfterWrite(CrawlURI curi, WARCWriter writer,
            long startPosition) {

        // Perform usual metadata update:
        super.updateMetadataAfterWrite(curi, writer, startPosition);

        // Also store the (compressed) record length:
        curi.addExtraInfo("warcRecordLength",
                (writer.getPosition() - startPosition));

    }

}
