/**
 * 
 */
package uk.bl.wap.modules.recrawl;

import java.io.IOException;

import org.archive.modules.CrawlURI;

/**
 * 
 * This extends the
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXPersistStoreProcessor
        extends OutbackCDXPersistLoadProcessor {

    @Override
    protected void innerProcess(CrawlURI uri) throws InterruptedException {
        try {
            // FIXME Implement thsi!
            this.ocdx.getCDX(uri.getURI());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

}
