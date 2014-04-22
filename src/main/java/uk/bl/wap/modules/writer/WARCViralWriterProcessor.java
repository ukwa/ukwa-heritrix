package uk.bl.wap.modules.writer;

import static org.archive.modules.CoreAttributeConstants.A_WARC_RESPONSE_HEADERS;
import static uk.bl.wap.io.warc.WARCConstants.VIRAL_CONTENT_MIMETYPE;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.ReplayInputStream;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.io.warc.WARCWriter;
import org.archive.modules.CrawlURI;
import org.archive.modules.writer.WARCWriterProcessor;
import org.archive.util.anvl.ANVLRecord;

import uk.bl.wap.io.XorInputStream;

/**
 * Minor variation of org.archive.modules.writer.WARCWriterProcessor to handle
 * conversion/viral records.
 * 
 * @author rcoram
 */

public class WARCViralWriterProcessor extends WARCWriterProcessor {
    private static final long serialVersionUID = 5818334757714399335L;

    @Override
    protected URI writeResponse(final WARCWriter w, final String timestamp,
	    final String mimetype, final URI baseid, final CrawlURI curi,
	    final ANVLRecord suppliedFields) throws IOException {
	ANVLRecord namedFields = suppliedFields;
	if (curi.getData().containsKey(A_WARC_RESPONSE_HEADERS)) {
	    namedFields = namedFields.clone();
	    for (Object headerObj : curi.getDataList(A_WARC_RESPONSE_HEADERS)) {
		String[] kv = StringUtils.split(((String) headerObj), ":", 2);
		namedFields.addLabelValue(kv[0].trim(), kv[1].trim());
	    }
	}

	WARCRecordInfo recordInfo = new WARCRecordInfo();
	recordInfo.setType(WARCRecordType.conversion);
	recordInfo.setUrl(curi.toString());
	recordInfo.setCreate14DigitDate(timestamp);
	recordInfo.setMimetype(VIRAL_CONTENT_MIMETYPE);
	recordInfo.setRecordId(baseid);
	recordInfo.setExtraHeaders(namedFields);
	recordInfo.setContentLength(curi.getRecorder().getRecordedInput()
		.getSize());
	recordInfo.setEnforceLength(true);

	ReplayInputStream ris = curi.getRecorder().getRecordedInput()
		.getReplayInputStream();
	recordInfo.setContentStream(new XorInputStream(ris));

	try {
	    w.writeRecord(recordInfo);
	} finally {
	    IOUtils.closeQuietly(ris);
	}

	return recordInfo.getRecordId();
    }
}