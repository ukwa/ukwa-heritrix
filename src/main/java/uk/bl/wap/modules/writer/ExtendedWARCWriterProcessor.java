/**
 * 
 */
package uk.bl.wap.modules.writer;

import java.util.logging.Logger;

import org.archive.io.warc.WARCWriter;
import org.archive.modules.CrawlURI;
import org.archive.modules.writer.WARCWriterChainProcessor;
import org.archive.modules.writer.WARCWriterProcessor;

/**
 * 
 * This just extends the WARC Writer to store the (compressed) WARC Record
 * length.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class ExtendedWARCWriterProcessor extends WARCWriterChainProcessor {

    private static final Logger logger = Logger
            .getLogger(ExtendedWARCWriterProcessor.class.getName());

    @Override
    protected void updateMetadataAfterWrite(CrawlURI curi, WARCWriter writer,
            long startPosition) {

        // Perform usual metadata update:
        super.updateMetadataAfterWrite(curi, writer, startPosition);

        // Also store the (compressed) record length:
        curi.addExtraInfo("warcFileRecordLength",
                (writer.getPosition() - startPosition));

    }
    
    public void addWebRenderRecords() {
    	
    }

}
