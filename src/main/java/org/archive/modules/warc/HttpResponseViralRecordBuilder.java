/**
 * 
 */
package org.archive.modules.warc;

import static org.archive.format.warc.WARCConstants.HEADER_KEY_IP;
import static org.archive.modules.CoreAttributeConstants.A_DNS_SERVER_IP_LABEL;

import java.io.IOException;
import java.net.URI;

import org.archive.format.warc.WARCConstants.WARCRecordType;
import org.archive.io.ReplayInputStream;
import org.archive.io.warc.WARCRecordInfo;
import org.archive.modules.CrawlURI;

import uk.bl.wap.io.XorInputStream;

/**
 * @author anj
 *
 */
public class HttpResponseViralRecordBuilder extends HttpResponseRecordBuilder {

	@Override
	public boolean shouldBuildRecord(CrawlURI curi) {
		return super.shouldBuildRecord(curi);
	}

	@Override
	public WARCRecordInfo buildRecord(CrawlURI curi, URI concurrentTo) throws IOException {
		// Setup record based on a straightforward Response record:
		WARCRecordInfo recordInfo = super.buildRecord(curi, concurrentTo);
		
        // Add in IP address if known:
        String ip = this.getHostAddress(curi);
        if (ip != null && ip.length() > 0) {
            recordInfo.addExtraHeader(HEADER_KEY_IP, ip);
        }

		// Turn it into a conversion record with an XORd stream:
		recordInfo.setType(WARCRecordType.conversion);		
		ReplayInputStream ris = curi.getRecorder().getRecordedInput()
				.getReplayInputStream();
		XorInputStream xor = new XorInputStream(ris);
			recordInfo.setContentStream(xor);
			recordInfo.setMimetype("application/http; encoding=bytewise_xor_with_"
				+ xor.getKey());
		
		return recordInfo;
	}
	
	

}
