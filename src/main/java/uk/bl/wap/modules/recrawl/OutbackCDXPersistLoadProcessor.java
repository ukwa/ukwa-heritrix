/*
 *  This file is part of the Heritrix web crawler (crawler.archive.org).
 *
 *  Licensed to the Internet Archive (IA) by one or more individual 
 *  contributors. 
 *
 *  The IA licenses this file to You under the Apache License, Version 2.0
 *  (the "License"); you may not use this file except in compliance with
 *  the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package uk.bl.wap.modules.recrawl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.Processor;
import org.archive.modules.recrawl.FetchHistoryHelper;
import org.archive.modules.recrawl.wbm.WbmPersistLoadProcessor;

import uk.bl.wap.util.OutbackCDXClient;

/**
 * A {@link Processor} for retrieving recrawl info from a remote OutbackCDX
 * instance.
 * 
 * As we're looking up the content hash, it could also a good time to look up
 * the last known crawl time-stamp. This could then be used to avoid re-crawling
 * URLs that have already been crawled within the requested time-frame, but have
 * been re-queued. The forceFetch field could be used to override this if
 * necessary.
 * 
 * This would be more efficient as we could avoid making this request when
 * processing candidate URIs. This would make frontier re-builds much faster
 * (e.g. after a crash), with the caveat that we will want to avoid repeatedly
 * forceFetching the same URLs over and over, so need to keep track of
 * forceFetch URIs during re-builds and only enqueue each URI once.
 *
 * 
 * <p>
 * Based on {@link WbmPersistLoadProcessor} by Kenji Nagahashi
 * </p>
 * 
 * @contributor Kenji Nagahashi
 * @author Andrew Jackson
 * 
 */
public class OutbackCDXPersistLoadProcessor extends Processor {
    private static final Logger logger = Logger
            .getLogger(OutbackCDXPersistLoadProcessor.class.getName());
    
    protected OutbackCDXClient outbackCDXClient = new OutbackCDXClient();

    public OutbackCDXClient getOutbackCDXClient() {
        return outbackCDXClient;
    }

    public void setOutbackCDXClient(OutbackCDXClient outbackCDXClient) {
        this.outbackCDXClient = outbackCDXClient;
    }

    private int historyLength = 2;

    public void setHistoryLength(int historyLength) {
        this.historyLength = historyLength;
    }
    public int getHistoryLength() {
        return historyLength;
    }

    // statistics
    private AtomicLong loadedCount = new AtomicLong();
    /**
     * number of times successfully loaded recrawl info.
     * @return long
     */
    public long getLoadedCount() {
        return loadedCount.get();
    }
    private AtomicLong missedCount = new AtomicLong();
    /**
     * number of times getting no recrawl info.
     * @return long
     */
    public long getMissedCount() {
        return missedCount.get();
    }
    private AtomicLong errorCount = new AtomicLong();
    /**
     * number of times cdx-server API call failed. 
     * @return long
     */
    public long getErrorCount() {
        return errorCount.get();
    }
    
    public OutbackCDXPersistLoadProcessor() {
    }
    
    @Override
    protected ProcessResult innerProcessResult(CrawlURI curi) throws InterruptedException {
        Map<String, Object> info = null;
        try {
            info = outbackCDXClient.getLastCrawl(curi.toString());
            logger.finest("GOT " + info + " for " + curi);
        } catch (Exception ex) {
            logger.log(Level.SEVERE, "Error calling OutbackCDX!", ex);
            errorCount.incrementAndGet();
        }
        if (info != null) {
            Map<String, Object> history = FetchHistoryHelper.getFetchHistory(curi,
                    (Long)info.get(FetchHistoryHelper.A_TIMESTAMP), historyLength);
            if (history != null)
                history.putAll(info);
            logger.finest("Now have history: " + history);
            loadedCount.incrementAndGet();
        } else {
            missedCount.incrementAndGet();
        }
        return ProcessResult.PROCEED;
    }

    /**
     * unused.
     */
    @Override
    protected void innerProcess(CrawlURI uri) throws InterruptedException {
    }

    @Override
    protected boolean shouldProcess(CrawlURI uri) {
        // TODO: we want deduplicate robots.txt, too.
        //if (uri.isPrerequisite()) return false;
        String scheme = uri.getUURI().getScheme();
        if (!(scheme.equals("http") || scheme.equals("https"))) return false;
        return true;
    }
    
}
