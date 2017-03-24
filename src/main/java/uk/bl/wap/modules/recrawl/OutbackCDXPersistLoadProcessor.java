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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.archive.modules.CoreAttributeConstants;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.Processor;
import org.archive.modules.recrawl.FetchHistoryHelper;
import org.archive.modules.recrawl.RecrawlAttributeConstants;
import org.archive.modules.recrawl.wbm.WbmPersistLoadProcessor;
import org.archive.util.ArchiveUtils;
import org.archive.util.DateUtils;

/**
 * A {@link Processor} for retrieving recrawl info from a remote OutbackCDX
 * instance.
 * <p>
 * Based on {@link WbmPersistLoadProcessor} by Kenji Nagahashi
 * </p>
 * 
 * @contributor Kenji Nagahashi.
 */
public class OutbackCDXPersistLoadProcessor extends Processor {
    private static final Log log = LogFactory
            .getLog(OutbackCDXPersistLoadProcessor.class);

    private HttpClient client;
    private PoolingHttpClientConnectionManager conman;

    private int historyLength = 2;

    public void setHistoryLength(int historyLength) {
        this.historyLength = historyLength;
    }
    public int getHistoryLength() {
        return historyLength;
    }

    private String outbackCdxPrefix = "http://localhost:9090/fc?url=";// "http://crawl-index/timeline?url=";

    private String contentDigestScheme = "sha1:";
    /**
     * set Content-Digest scheme string to prepend to the hash string found in CDX.
     * Heritrix's Content-Digest comparison including this part.
     * {@code "sha1:"} by default.
     * @param contentDigestScheme
     */
    public void setContentDigestScheme(String contentDigestScheme) {
        this.contentDigestScheme = contentDigestScheme;
    }
    public String getContentDigestScheme() {
        return contentDigestScheme;
    }
    private int socketTimeout = 10000;
    /**
     * socket timeout (SO_TIMEOUT) for HTTP client in milliseconds.
     */
    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }
    public int getSocketTimeout() {
        return socketTimeout;
    }

    private int connectionTimeout = 10000;
    /**
     * connection timeout for HTTP client in milliseconds. 
     * @param connectionTimeout
     */
    public void setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
    }
    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    private int maxConnections = 10;
    public int getMaxConnections() {
        return maxConnections;
    }
    public synchronized void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
        if (conman != null) {
            if (conman.getMaxTotal() < this.maxConnections)
                conman.setMaxTotal(this.maxConnections);
            conman.setDefaultMaxPerRoute(this.maxConnections);
        }
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
    
    private AtomicLong cumulativeFetchTime = new AtomicLong();
    /**
     * total milliseconds spent in API call.
     * it is a sum of time waited for next available connection,
     * and actual HTTP request-response round-trip, across all threads.
     * @return
     */
    public long getCumulativeFetchTime() {
        return cumulativeFetchTime.get();
    }

    public void setHttpClient(HttpClient client) {
        this.client = client;
    }

    public synchronized HttpClient getHttpClient() {
        if (client == null) {
            if (conman == null) {
                conman = new PoolingHttpClientConnectionManager();
                conman.setDefaultMaxPerRoute(maxConnections);
                conman.setMaxTotal(Math.max(conman.getMaxTotal(), maxConnections));
            }
            HttpClientBuilder builder = HttpClientBuilder.create()
                    .disableCookieManagement()
                    .setConnectionManager(conman);
            builder.useSystemProperties();
            // And build:
            this.client = builder.build();
        }
        return client;
    }

    private long queryRangeSecs = 6L*30*24*3600;
    /**
     * 
     * @param queryRangeSecs
     */
    public void setQueryRangeSecs(long queryRangeSecs) {
        this.queryRangeSecs = queryRangeSecs;
    }
    public long getQueryRangeSecs() {
        return queryRangeSecs;
    }

    private String buildStartDate() {
        final long range = queryRangeSecs;
        if (range <= 0)
            return ArchiveUtils.get14DigitDate(new Date(0));
        Date now = new Date();
        Date startDate = new Date(now.getTime() - range*1000);
        return ArchiveUtils.get14DigitDate(startDate);
    }
    
    protected String buildURL(String url) {
        // we don't need to pass scheme part, but no problem passing it.
        StringBuilder sb = new StringBuilder();
        sb.append(this.outbackCdxPrefix);
        String encodedURL;
        try {
            encodedURL = URLEncoder.encode(url, "UTF-8");
        } catch (UnsupportedEncodingException ex) {
            encodedURL = url;
        }
        sb.append(encodedURL);
        // sb.append(buildStartDate())
        return sb.toString();
    }

    public OutbackCDXPersistLoadProcessor() {
    }
    
    protected InputStream getCDX(String qurl) throws InterruptedException, IOException {
        final String url = buildURL(qurl);
        HttpGet m = new HttpGet(url);
        m.setConfig(RequestConfig.custom().setConnectTimeout(connectionTimeout)
                .setSocketTimeout(socketTimeout).build());
        HttpEntity entity = null;
        int attempts = 0;
        do {
            if (Thread.interrupted())
                throw new InterruptedException("interrupted while GET " + url);
            if (attempts > 0) {
                Thread.sleep(5000);
            }
            try {
                long t0 = System.currentTimeMillis();
                HttpResponse resp = getHttpClient().execute(m);
                cumulativeFetchTime.addAndGet(System.currentTimeMillis() - t0);
                StatusLine sl = resp.getStatusLine();
                if (sl.getStatusCode() != 200) {
                    log.error("GET " + url + " failed with status=" + sl.getStatusCode() + " " + sl.getReasonPhrase());
                    entity = resp.getEntity();
                    entity.getContent().close();
                    entity = null;
                    continue;
                }
                entity = resp.getEntity();
            } catch (IOException ex) {
                log.error("GEt " + url + " failed with error " + ex.getMessage());
            } catch (Exception ex) {
                log.error("GET " + url + " failed with error ", ex);
            }
        } while (entity == null && ++attempts < 3);
        if (entity == null) {
            throw new IOException("giving up on GET " + url + " after " + attempts + " attempts");
        }
        return entity.getContent();
    }
    
    @Override
    protected ProcessResult innerProcessResult(CrawlURI curi) throws InterruptedException {
        InputStream is;
        try {
            is = getCDX(curi.toString());
        } catch (IOException ex) {
            log.error(ex.getMessage());
            errorCount.incrementAndGet();
            return ProcessResult.PROCEED;
        }
        Map<String, Object> info = null;
        try {
            info = getLastCrawl(is);
        } catch (IOException ex) {
            log.error("error parsing response", ex);
        } finally {
            if (is != null)
                ArchiveUtils.closeQuietly(is);
        }
        if (info != null) {
            Map<String, Object> history = FetchHistoryHelper.getFetchHistory(curi,
                    (Long)info.get(FetchHistoryHelper.A_TIMESTAMP), historyLength);
            if (history != null)
                history.putAll(info);
            loadedCount.incrementAndGet();
        } else {
            missedCount.incrementAndGet();
        }
        return ProcessResult.PROCEED;
    }

    protected HashMap<String, Object> getLastCrawl(InputStream is) throws IOException {
        // read CDX lines, save most recent (at the end) hash.
        ByteBuffer buffer = ByteBuffer.allocate(32);
        ByteBuffer tsbuffer = ByteBuffer.allocate(14);
        int field = 0;
        int c;
        do {
            c = is.read();
            if (field == 1) {
                // 14-digits timestamp
                tsbuffer.clear();
                while (Character.isDigit(c) && tsbuffer.remaining() > 0) {
                    tsbuffer.put((byte)c);
                    c = is.read();
                }
                if (c != ' ' || tsbuffer.position() != 14) {
                    tsbuffer.clear();
                }
                // fall through to skip the rest
            } else if (field == 5) {
                buffer.clear();
                while ((c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') && buffer.remaining() > 0) {
                    buffer.put((byte)c);
                    c = is.read();
                }
                if (c != ' ' || buffer.position() != 32) {
                    buffer.clear();
                }
                // fall through to skip the rest
            }
            while (true) {
                if (c == -1) {
                    break;
                } else if (c == '\n') {
                    field = 0;
                    break;
                } else if (c == ' ') {
                    field++;
                    break;
                }
                c = is.read();
            }
        } while (c != -1);

        HashMap<String, Object> info = new HashMap<String, Object>();
        if (buffer.remaining() == 0) {
            info.put(RecrawlAttributeConstants.A_CONTENT_DIGEST, contentDigestScheme + new String(buffer.array()));
        }
        if (tsbuffer.remaining() == 0) {
            try {
                long ts = DateUtils.parse14DigitDate(new String(tsbuffer.array())).getTime();
                // A_TIMESTAMP has been used for sorting history long before A_FETCH_BEGAN_TIME
                // field was introduced. Now FetchHistoryProcessor fails if A_FETCH_BEGAN_TIME is
                // not set. We could stop storing A_TIMESTAMP and sort by A_FETCH_BEGAN_TIME.
                info.put(FetchHistoryHelper.A_TIMESTAMP, ts);
                info.put(CoreAttributeConstants.A_FETCH_BEGAN_TIME, ts);
            } catch (ParseException ex) {
            }
        }
        return info.isEmpty() ? null : info;
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
    
    /**
     * main entry point for quick test.
     * @param args
     */
    public static void main(String[] args) throws Exception {
        String url = args[0];
        OutbackCDXPersistLoadProcessor wp = new OutbackCDXPersistLoadProcessor();
        InputStream is = wp.getCDX(url);
        HashMap<String, Object> info = wp.getLastCrawl(is);
        is.close();
        System.out.println("Info: " + info);
    }
}
