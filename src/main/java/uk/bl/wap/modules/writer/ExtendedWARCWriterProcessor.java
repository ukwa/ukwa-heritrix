/**
 * 
 */
package uk.bl.wap.modules.writer;

import java.util.logging.Logger;

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

    private static final Logger logger = Logger
            .getLogger(ExtendedWARCWriterProcessor.class.getName());

    // Sketch of how to close the pool on SIGTERM
    // (not in use as when running in Docker the JVM does not get the SIGTERM)
    // public ExtendedWARCWriterProcessor() {
    // // Register a shotdown hook:
    // logger.severe("Registering shutdown hook...");
    // Runtime.getRuntime().addShutdownHook(new Thread() {
    // public void run() {
    // logger.severe("Running shutdown hook...");
    // // Close the pool:
    // if (getPool() != null) {
    // logger.warning("Attempting to close WARC writer pool...");
    // getPool().close();
    // }
    // }
    // });
    // }

    @Override
    protected void updateMetadataAfterWrite(CrawlURI curi, WARCWriter writer,
            long startPosition) {

        // Perform usual metadata update:
        super.updateMetadataAfterWrite(curi, writer, startPosition);

        // Also store the (compressed) record length:
        curi.addExtraInfo("warcFileRecordLength",
                (writer.getPosition() - startPosition));

    }

}
