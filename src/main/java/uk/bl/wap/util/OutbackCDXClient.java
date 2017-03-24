/**
 * 
 */
package uk.bl.wap.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.util.Date;
import java.util.HashMap;
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
import org.archive.modules.recrawl.FetchHistoryHelper;
import org.archive.modules.recrawl.RecrawlAttributeConstants;
import org.archive.util.ArchiveUtils;
import org.archive.util.DateUtils;
import org.archive.util.MimetypeUtils;
import org.json.JSONObject;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXClient {
    private static final Log log = LogFactory.getLog(OutbackCDXClient.class);

    private HttpClient client;
    private PoolingHttpClientConnectionManager conman;

    private String outbackCdxPrefix = "http://localhost:9090/fc?url=";// "http://crawl-index/timeline?url=";

    private int socketTimeout = 10000;

    private String contentDigestScheme = "sha1:";

    /**
     * set Content-Digest scheme string to prepend to the hash string found in
     * CDX. Heritrix's Content-Digest comparison including this part.
     * {@code "sha1:"} by default.
     * 
     * @param contentDigestScheme
     */
    public void setContentDigestScheme(String contentDigestScheme) {
        this.contentDigestScheme = contentDigestScheme;
    }

    public String getContentDigestScheme() {
        return contentDigestScheme;
    }

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
     * 
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

    private AtomicLong cumulativeFetchTime = new AtomicLong();

    /**
     * total milliseconds spent in API call. it is a sum of time waited for next
     * available connection, and actual HTTP request-response round-trip, across
     * all threads.
     * 
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
                conman.setMaxTotal(
                        Math.max(conman.getMaxTotal(), maxConnections));
            }
            HttpClientBuilder builder = HttpClientBuilder.create()
                    .disableCookieManagement().setConnectionManager(conman);
            builder.useSystemProperties();
            // And build:
            this.client = builder.build();
        }
        return client;
    }

    private long queryRangeSecs = 6L * 30 * 24 * 3600;

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
        Date startDate = new Date(now.getTime() - range * 1000);
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

    public InputStream getCDX(String qurl)
            throws InterruptedException, IOException {
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
                    log.error("GET " + url + " failed with status="
                            + sl.getStatusCode() + " " + sl.getReasonPhrase());
                    entity = resp.getEntity();
                    entity.getContent().close();
                    entity = null;
                    continue;
                }
                entity = resp.getEntity();
            } catch (IOException ex) {
                log.error(
                        "GEt " + url + " failed with error " + ex.getMessage());
            } catch (Exception ex) {
                log.error("GET " + url + " failed with error ", ex);
            }
        } while (entity == null && ++attempts < 3);
        if (entity == null) {
            throw new IOException("giving up on GET " + url + " after "
                    + attempts + " attempts");
        }
        return entity.getContent();
    }

    /**
     * 
     * Output e.g. {.ts=1486853587000,
     * content-digest=sha1:G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK,
     * fetch-began-time=1486853587000}
     * 
     * @param is
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    public HashMap<String, Object> getLastCrawl(String qurl)
            throws IOException, InterruptedException {
        // Perform the query:
        InputStream is = this.getCDX(qurl);
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
                    tsbuffer.put((byte) c);
                    c = is.read();
                }
                if (c != ' ' || tsbuffer.position() != 14) {
                    tsbuffer.clear();
                }
                // fall through to skip the rest
            } else if (field == 5) {
                buffer.clear();
                while ((c >= 'A' && c <= 'Z' || c >= '0' && c <= '9')
                        && buffer.remaining() > 0) {
                    buffer.put((byte) c);
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

        // Shut down input stream
        if (is != null)
            ArchiveUtils.closeQuietly(is);

        // Put into usable form:
        HashMap<String, Object> info = new HashMap<String, Object>();
        if (buffer.remaining() == 0) {
            info.put(RecrawlAttributeConstants.A_CONTENT_DIGEST,
                    contentDigestScheme + new String(buffer.array()));
        }
        if (tsbuffer.remaining() == 0) {
            try {
                long ts = DateUtils
                        .parse14DigitDate(new String(tsbuffer.array()))
                        .getTime();
                // A_TIMESTAMP has been used for sorting history long before
                // A_FETCH_BEGAN_TIME
                // field was introduced. Now FetchHistoryProcessor fails if
                // A_FETCH_BEGAN_TIME is
                // not set. We could stop storing A_TIMESTAMP and sort by
                // A_FETCH_BEGAN_TIME.
                info.put(FetchHistoryHelper.A_TIMESTAMP, ts);
                info.put(CoreAttributeConstants.A_FETCH_BEGAN_TIME, ts);
            } catch (ParseException ex) {
            }
        }
        return info.isEmpty() ? null : info;
    }

    public void putUri(CrawlURI curi) {
        // FIXME Put this URI into OutbackCDX
    }

    /**
     * 
     * 11 field format:
     * 
     * <pre>
     * urlkey timestamp original mimetype status digest redirecturl robots length compressedoffset file
     * </pre>
     * 
     * (urlkey overridden, can be -)
     * 
     * <pre>
     * au,gov,australia)/about 20070831172339 http://australia.gov.au/about text/html 200 ZUEQ3STH3JAEABZG22LQI626TTY7DN2A - - - 14369759 NLA-AU-CRAWL-002-20070831172246-04117-crawling015.us.archive.org.arc.gz
     * </pre>
     * 
     * @param curi
     * @return
     */
    private static String toCDXLine(CrawlURI curi) {
        JSONObject jo = new JSONObject();

        jo.put("timestamp",
                ArchiveUtils.getLog17Date(System.currentTimeMillis()));

        jo.put("content_length",
                curi.isHttpTransaction() && curi.getContentLength() >= 0
                        ? curi.getContentLength() : JSONObject.NULL);
        jo.put("size", curi.getContentSize() > 0 ? curi.getContentSize()
                : JSONObject.NULL);

        jo.put("status_code", checkForNull(curi.getFetchStatus()));
        jo.put("url", checkForNull(curi.getUURI().toString()));
        jo.put("hop_path", checkForNull(curi.getPathFromSeed()));
        jo.put("via", checkForNull(curi.flattenVia()));
        jo.put("mimetype",
                checkForNull(MimetypeUtils.truncate(curi.getContentType())));
        jo.put("thread", checkForNull(curi.getThreadNumber()));

        if (curi.containsDataKey(
                CoreAttributeConstants.A_FETCH_COMPLETED_TIME)) {
            long beganTime = curi.getFetchBeginTime();
            String fetchBeginDuration = ArchiveUtils.get17DigitDate(beganTime)
                    + "+" + (curi.getFetchCompletedTime() - beganTime);
            jo.put("start_time_plus_duration", fetchBeginDuration);
        } else {
            jo.put("start_time_plus_duration", JSONObject.NULL);
        }

        jo.put("content_digest",
                checkForNull(curi.getContentDigestSchemeString()));
        jo.put("seed", checkForNull(curi.getSourceTag()));

        JSONObject ei = curi.getExtraInfo();
        if (ei == null) {
            ei = new JSONObject();
        }
        // copy so we can remove unrolled fields
        ei = new JSONObject(curi.getExtraInfo().toString());
        ei.remove("contentSize"); // we get this value above
        jo.put("warc_filename", checkForNull(ei.remove("warcFilename")));
        jo.put("warc_offset", checkForNull(ei.remove("warcFileOffset")));
        jo.put("warc_length", checkForNull(ei.remove("warcRecordLength")));
        jo.put("extra_info", ei);

        return jo.toString();
    }

    protected static Object checkForNull(Object o) {
        return o != null ? o : JSONObject.NULL;
    }

    /**
     * main entry point for quick test.
     * 
     * @param args
     */
    public static void main(String[] args) throws Exception {
        String url = args[0];
        OutbackCDXClient wp = new OutbackCDXClient();
        HashMap<String, Object> info = wp.getLastCrawl(url);
        System.out.println("Info: " + info);
    }

}
