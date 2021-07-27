/**
 * 
 */
package uk.bl.wap.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.IDN;
import java.net.URISyntaxException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.conn.HttpClientConnectionManager;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.archive.format.cdx.CDXLine;
import org.archive.format.cdx.StandardCDXLineFactory;
import org.archive.modules.CoreAttributeConstants;
import org.archive.modules.CrawlURI;
import org.archive.modules.recrawl.FetchHistoryHelper;
import org.archive.modules.recrawl.RecrawlAttributeConstants;
import org.archive.net.UURIFactory;
import org.archive.util.ArchiveUtils;
import org.archive.util.DateUtils;
import org.archive.util.MimetypeUtils;
import org.json.JSONObject;

import com.esotericsoftware.minlog.Log;

import io.prometheus.client.Counter;
import uk.bl.wap.modules.recrawl.OutbackCDXPersistLoadProcessor;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class OutbackCDXClient {
    private static final Logger logger = Logger
            .getLogger(OutbackCDXPersistLoadProcessor.class.getName());

    private PoolingHttpClientConnectionManager pcm = null;

    private ThreadLocal<CloseableHttpClient> client = new ThreadLocal<CloseableHttpClient>();

    private String endpoint = "http://localhost:9090/fc";// ?url=";//
                                                         // "http://crawl-index/timeline?url=";
    protected StandardCDXLineFactory cdxLineFactory = new StandardCDXLineFactory(
            "cdx11");

    public String getEndpoint() {
        return endpoint;
    }

    public void setEndpoint(String endpoint) {
        this.endpoint = endpoint;
    }

    private boolean useSystemProperties = false;

    /**
     * @return the useSystemProperties
     */
    public boolean isUseSystemProperties() {
        return useSystemProperties;
    }

    /**
     * @param useSystemProperties
     *            the useSystemProperties to set
     */
    public void setUseSystemProperties(boolean useSystemProperties) {
        this.useSystemProperties = useSystemProperties;
    }

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

    private int socketTimeout = 30000;

    /**
     * socket timeout (SO_TIMEOUT) for HTTP client in milliseconds.
     */
    public void setSocketTimeout(int socketTimeout) {
        this.socketTimeout = socketTimeout;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    private int connectionTimeout = 30000;

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

    private int maxConnections = 100;

    public int getMaxConnections() {
        return maxConnections;
    }

    public synchronized void setMaxConnections(int maxConnections) {
        this.maxConnections = maxConnections;
    }

    private boolean usePooledConnectionManager = false;

    /**
     * @return the usePooledConnectionManager
     */
    public boolean isUsePooledConnectionManager() {
        return usePooledConnectionManager;
    }

    /**
     * @param usePooledConnectionManager
     *            the usePooledConnectionManager to set
     */
    public void setUsePooledConnectionManager(
            boolean usePooledConnectionManager) {
        this.usePooledConnectionManager = usePooledConnectionManager;
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

    /**
     * Set up a connection manager using the required settings:
     * 
     * @return
     */
    private HttpClientConnectionManager getConnectionManager() {
        if (usePooledConnectionManager) {
            if (pcm == null) {
                pcm = new PoolingHttpClientConnectionManager();
                pcm.setDefaultMaxPerRoute(maxConnections);
                pcm.setMaxTotal(Math.max(pcm.getMaxTotal(), maxConnections));
            }
            return pcm;

        } else {
            return new BasicHttpClientConnectionManager();
        }
    }

    /*
     * Return a separate client for every connection:
     */
    private CloseableHttpClient getHttpClient() {
        if (this.client.get() == null) {
            // Allow client to look up system properties for proxy settings etc.
            // Defaults to false as this can lead to thread contention.
            if (useSystemProperties) {
                client.set(HttpClients.custom()
                        .setConnectionManager(getConnectionManager())
                        // .disableConnectionState()
                        .disableCookieManagement().useSystemProperties()
                        .build());
            } else {
                client.set(HttpClients.custom()
                        .setConnectionManager(getConnectionManager())
                        // .disableConnectionState()
                        .disableCookieManagement().build());
            }
        }
        return client.get();
    }

    private long queryRangeSecs = 6L * 30 * 24 * 3600;

    private static final Counter requestTotals = Counter.build()
            .name("outbackcdx_requests_total").labelNames("kind", "status")
            .help("Total OutbackCDX requests, query or post.").register();

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

    protected String buildURL(String url, int limit, boolean mostRecentFirst)
            throws URISyntaxException {
        URIBuilder uriBuilder = new URIBuilder(this.endpoint);
        uriBuilder.addParameter("limit", "" + limit);
        uriBuilder.addParameter("matchType", "exact");
        // Don't use crawl failures for recentlySeen check/digest fetch:
        uriBuilder.addParameter("filter", "!mimetype:failure-status-code.*");
        if (mostRecentFirst) {
            uriBuilder.addParameter("sort", "reverse");
        }
        uriBuilder.addParameter("url", url);
        return uriBuilder.build().toString();
    }

    private CDXLine getCDXLine(String qurl, int limit,
            boolean mostRecentFirst)
            throws InterruptedException, IOException, URISyntaxException {
        final String url = buildURL(qurl, limit, mostRecentFirst);
        logger.fine("GET " + url);
        //
        CloseableHttpResponse resp = null;
        HttpEntity entity = null;
        BufferedReader br = null;
        //
        int attempts = 0;
        do {
            if (Thread.interrupted())
                throw new InterruptedException("interrupted while GET " + url);
            if (attempts > 0) {
                Thread.sleep(5000);
            }
            try {
                HttpGet m = new HttpGet(url);
                m.setConfig(RequestConfig.custom()
                        .setConnectTimeout(connectionTimeout)
                        .setSocketTimeout(socketTimeout).build());
                long t0 = System.currentTimeMillis();
                resp = getHttpClient().execute(m);
                cumulativeFetchTime.addAndGet(System.currentTimeMillis() - t0);
                StatusLine sl = resp.getStatusLine();
                requestTotals.labels("query", "" + sl.getStatusCode()).inc();
                // Get the entity
                entity = resp.getEntity();
                if (sl.getStatusCode() != 200) {
                    if (sl.getStatusCode() == 404) {
                        logger.info(
                                "Got a 404: the collection has probably not been created yet.");
                        return null;
                    } else {
                        logger.severe("GET " + url + " failed with status="
                                + sl.getStatusCode() + " "
                                + sl.getReasonPhrase());
                    }
                    continue;
                } else {
                    // read CDX lines, recover the most recent (first) hash and
                    // timestamp. Only permit exact URL matches, skip revisit
                    // records.
                    br = new BufferedReader(
                            new InputStreamReader(entity.getContent()));
                    String cdxLine;
                    while ((cdxLine = br.readLine()) != null) {
                        CDXLine line = cdxLineFactory
                                .createStandardCDXLine(cdxLine);
                        // This could be a revisit record, but the hash should
                        // always be the original payload hash, so use it:
                        if (line.getOriginalUrl().equals(qurl)) {
                            return line;
                        }
                    }
                    // The lookup worked, but no match:
                    return null;
                }
            } catch (IOException ex) {
                logger.severe(
                        "GET " + url + " failed with error " + ex.getMessage());
                ex.printStackTrace();
                requestTotals.labels("query", "IOException").inc();
            } catch (Exception ex) {
                logger.log(Level.SEVERE, "GET " + url + " failed with error ",
                        ex);
                ex.printStackTrace();
                requestTotals.labels("query",
                        "Exception: " + ex.getClass().getName()).inc();
            } finally {
                // Shut down cleanly:
                if (entity != null)
                    EntityUtils.consumeQuietly(entity);
                if (br != null)
                    ArchiveUtils.closeQuietly(br);
                if (resp != null)
                    ArchiveUtils.closeQuietly(resp);
            }
        } while (++attempts < 3);

        // That didn't work then!
        throw new IOException(
                "giving up on GET " + url + " after "
                    + attempts + " attempts");
    }

    /**
     * 
     * Output e.g. {.ts=1486853587000,
     * content-digest=sha1:G7HRM7BGOKSKMSXZAHMUQTTV53QOFSMK,
     * fetch-began-time=1486853587000}
     * 
     * FIXME This needs to pull more than one line and filter for an exact URL
     * match.
     * 
     * @param is
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws URISyntaxException
     */
    public HashMap<String, Object> getLastCrawl(String qurl)
            throws IOException, InterruptedException, URISyntaxException {
        // Check valid:
        if (!checkValid(qurl)) {
            return null;
        }
        
        String hash = null;
        String timestamp = null;

        CDXLine line = this.getCDXLine(qurl, 100, true);
        if (line != null) {
            timestamp = line.getTimestamp();
            hash = line.getDigest();
        }

        // Put into usable form:
        HashMap<String, Object> info = new HashMap<String, Object>();
        if (hash != null) {
            info.put(RecrawlAttributeConstants.A_CONTENT_DIGEST,
                    contentDigestScheme + hash);
        }
        if (timestamp!= null) {
            try {
                long ts = DateUtils
                        .parse14DigitDate(timestamp)
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
                Log.error(
                        "Could not parse '" + timestamp + " as 14-digit date!");
            }
        }
        return info.isEmpty() ? null : info;
    }

    /**
     * 
     * @param uri
     * @return
     */
    protected boolean checkValid(String uri) {
        if (uri.startsWith("http")) {
            try {
                CrawlURI candidate = new CrawlURI(UURIFactory.getInstance(uri));
                String host = candidate.getUURI().getHost();
                if (host == null) {
                    return false;
                }
                String idn_host = IDN.toASCII(host);
                logger.finest("Parsed URI and host successfully: " + idn_host);
            } catch (URIException e) {
                logger.warning("Could not parse URI: " + uri);
                return false;
            } catch (IllegalArgumentException e) {
                logger.warning(
                        "Could not parse host from: " + uri);
                return false;
            }
        }        
        return true;
    }

    /**
     * Put this URI into OutbackCDX
     * 
     * @param curi
     */
    public void putUri(CrawlURI curi) {
        // Check valid:
        if (!checkValid(curi.getURI())) {
            return;
        }

        // Re-format as CDX-11 string:
        String cdx11 = toCDXLine(curi);
        logger.fine("POSTING: " + cdx11);

        // Use the shared client:
        CloseableHttpClient client = getHttpClient();

        // Resources to clear:
        CloseableHttpResponse httpResponse = null;
        HttpEntity entity = null;

        // Retry loop in case of service problems:
        boolean retry = true;
        while (retry) {
            try {
                // POST to the endpoint:
                HttpPost postRequest = new HttpPost(endpoint);
                postRequest.addHeader("Content-Type",
                        "application/x-www-form-urlencoded");
                StringEntity userEntity = new StringEntity(cdx11, "UTF-8");
                postRequest.setEntity(userEntity);
                // Perform the POST
                httpResponse = client.execute(postRequest);
                // Get the result:
                int statusCode = httpResponse.getStatusLine().getStatusCode();
                // Including the body, to ensure the connection can be released:
                entity = httpResponse.getEntity();
                // Report if all went well:
                if (statusCode == 200) {
                    String apiOutput = EntityUtils.toString(entity, "UTF-8");
                    logger.finest("Sent the record, got: " + apiOutput);
                    // It worked! No need to retry:
                    retry = false;
                } else {
                    logger.warning("Got response code: " + statusCode);
                }
                // Report metric for monitoring:
                requestTotals.labels("post", "" + statusCode).inc();
                httpResponse.close();

            } catch (Exception e) {
                logger.log(Level.WARNING, "POSTing results for " + curi.getURI()
                        + " failed with exception:", e);
                requestTotals
                        .labels("post", "Exception: " + e.getClass().getName())
                        .inc();
                try {
                    // Sleep before trying again...
                    Thread.sleep(1000 * 10);
                } catch (InterruptedException e1) {
                    // TODO Auto-generated catch block
                    e1.printStackTrace();
                }
            } finally {
                // Shut down cleanly:
                if (entity != null)
                    EntityUtils.consumeQuietly(entity);
                if (httpResponse != null)
                    ArchiveUtils.closeQuietly(httpResponse);
            }
        }

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
        // Pick out the fetch timestamp:
        String crawl_timestamp = "-";
        if (curi.containsDataKey(
                CoreAttributeConstants.A_FETCH_COMPLETED_TIME)) {
            long beganTime = curi.getFetchBeginTime();
            crawl_timestamp = ArchiveUtils.get14DigitDate(beganTime);
        } else {
            // Use now as the event date if there isn't one:
            crawl_timestamp = ArchiveUtils
                    .get14DigitDate(System.currentTimeMillis());
        }
        // Be explicit about de-duplicated resources:
        String content_type = MimetypeUtils.truncate(curi.getContentType());
        if (curi.getAnnotations().contains("duplicate:digest")) {
            content_type = "warc/revisit";
        }
        // Clean up -ve fetch status as OutbackCDX status does not store -ve
        // status:
        int fetch_status = curi.getFetchStatus();
        if (fetch_status < 0) {
            content_type = "failure-status-code/" + fetch_status;
            fetch_status = 599;
        }
        // Pick out the WARC information:
        JSONObject jei = curi.getExtraInfo();
        String warc_filename = jei.optString("warcFilename", "-");
        String warc_offset = jei.optString("warcFileOffset", "0");
        String warc_length = jei.optString("warcFileRecordLength", "0");
        // Format as CDX-11:
        StringBuffer sb = new StringBuffer();
        sb.append("- ");
        sb.append(crawl_timestamp);
        sb.append(" ");
        sb.append(curi.getUURI());
        sb.append(" ");
        sb.append(content_type);
        sb.append(" ");
        sb.append(fetch_status);
        sb.append(" ");
        sb.append(curi.getContentDigestString());
        sb.append(" ");
        sb.append(curi.flattenVia());
        sb.append(" ");
        sb.append("-"); // Robots field
        sb.append(" ");
        sb.append(warc_length);
        sb.append(" ");
        sb.append(warc_offset);
        sb.append(" ");
        sb.append(warc_filename);

        return sb.toString();
    }

    /**
     * main entry point for a quick test.
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
