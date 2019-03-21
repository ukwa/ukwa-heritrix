package uk.bl.wap.crawler.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.io.IOUtils;
import org.archive.crawler.event.CrawlStateEvent;
import org.archive.crawler.framework.CrawlController;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.Processor;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;
import org.archive.modules.fetcher.FetchStatusCodes;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.archive.spring.PathSharingContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

/**
 * An alternative to FetchHTTP & ExtractHTML
 * 
 * Not dissimilar to
 * {@link https://github.com/adam-miller/ExternalBrowserExtractorHTML} but based
 * on using an external HTTP service to aid isolation and load-balancing.
 * 
 * The HTTP endpoint implementation is here:
 * {@link https://github.com/ukwa/webrender-phantomjs}
 * 
 * Expects the browser rendering process to handle archiving the downloaded
 * resource e.g. via warcprox.
 * 
 * TODO consider adding a downloadViaHeritrix option for those without warcprox
 * to hand.
 * 
 * TODO Use the H3 retry/deferral logic, as per
 * org.archive.crawler.frontier.AbstractFrontier.needsReenqueuing(CrawlURI)
 * rather than retrying directly (which recrawls a lot). Instead, use annotation
 * to note number of retries and when browser rendering keeps failing, allow
 * FetchHTTP to handle it instead.
 * 
 */
public class WrenderProcessor extends Processor implements
        ApplicationContextAware,
        ApplicationListener<ApplicationEvent> {

    private int connectTimeout = 5 * 60 * 1000; // Default 5 minutes

    private int readTimeout = 20 * 60 * 1000; // Default 20 minutes

    private int maxTries = 1; // How many times to attempt web-rendering before
                              // deferring to plain FetchHTTP

    private String warcFilePrefix = "WREN"; // Tag to put at start of WARC
                                              // filenames

    private String wrenderEndpoint = "http://localhost:8000/render";

    private PathSharingContext psc = null;
    private String launchId = null;

    private final static Logger LOGGER = Logger
            .getLogger(WrenderProcessor.class.getName());

    public static final String HTTP_SCHEME = "http";
    public static final String HTTPS_SCHEME = "https";
    public static final String TRIES_ANNOTATION_PREFIX = "WebRenderCount:";

    protected CrawlController controller;

    public CrawlController getCrawlController() {
        return this.controller;
    }

    @Autowired
    public void setCrawlController(CrawlController controller) {
        this.controller = controller;
    }

    public int getConnectTimeout() {
        return connectTimeout;
    }

    /**
     * @param connectTimeout
     *            connection timeout in microseconds, e.g. 1 minute = 60 * 1000
     */
    public void setConnectTimeout(int connectTimeout) {
        this.connectTimeout = connectTimeout;
    }

    public int getReadTimeout() {
        return readTimeout;
    }

    /**
     * @param readTimeout
     *            read timeout in microseconds, e.g. 1 minute = 60 * 1000
     */
    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public int getMaxTries() {
        return maxTries;
    }

    /**
     * @param maxTries
     *            maximum number of times to retry the web render of this URL.
     */
    public void setMaxTries(int maxTries) {
        this.maxTries = maxTries;
    }

    /**
     * 
     * @return
     */
    public String getWrenderEndpoint() {
        return wrenderEndpoint;
    }

    /**
     * 
     * @param wrenderEndpoint
     */
    public void setWrenderEndpoint(String wrenderEndpoint) {
        this.wrenderEndpoint = wrenderEndpoint;
    }

    /**
     * 
     * @return
     */
    public String getWarcFilePrefix() {
        return warcFilePrefix;
    }

    /**
     * 
     * @param warcFilePrefix
     */
    public void setWarcFilePrefix(String warcFilePrefix) {
        this.warcFilePrefix = warcFilePrefix;
    }

    /**
     * Can this processor fetch the given CrawlURI. May set a fetch status if
     * this processor would usually handle the CrawlURI, but cannot in this
     * instance.
     * 
     * @param curi
     * @return True if processor can fetch.
     */
    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        String scheme = curi.getUURI().getScheme();
        if (!(scheme.equals(HTTP_SCHEME) || scheme.equals(HTTPS_SCHEME))) {
            // handles only plain http and https
            return false;
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.archive.modules.Processor#innerProcess(org.archive.modules.CrawlURI)
     */
    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        throw new AssertionError(
                "This method should not be called as we have overridden innerProcessResult()!");
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.archive.modules.Processor#innerProcessResult(org.archive.modules.
     * CrawlURI)
     */
    @Override
    public ProcessResult innerProcessResult(CrawlURI curi)
            throws InterruptedException {
        // Get # attempts to use the web renderer so far.
        int tries = getTriesFromCrawlURI(curi);
        // Should we keep trying to use the web renderer?
        if (tries < maxTries) {
            // Attempt to render and extract links using a web-rendering
            // service:
            curi.setFetchBeginTime(System.currentTimeMillis());
            this.doWrender(curi);
            curi.setFetchCompletedTime(System.currentTimeMillis());
            setTriesForCrawlURI(curi, tries + 1);
            // Jump past the usual fetchers/extractors, to finish or retry
            // later:
            return ProcessResult.FINISH;
        } else {
            // Let the usual H3 process chain handle this URL:
            return ProcessResult.PROCEED;
        }
    }

    private void setTriesForCrawlURI(CrawlURI curi, int i) {
        // Remove any existing annotation:
        String toRemove = getTriesAnnotation(curi);
        if (toRemove != null) {
            curi.getAnnotations().remove(toRemove);
        }

        // Add updated annotation:
        curi.getAnnotations().add(TRIES_ANNOTATION_PREFIX + Integer.toString(i));
    }

    private String getTriesAnnotation(CrawlURI curi) {
        for (String annot : curi.getAnnotations()) {
            if (annot.startsWith(TRIES_ANNOTATION_PREFIX)) {
                return annot;
            }
        }
        return null;
    }

    private int getTriesFromCrawlURI(CrawlURI curi) {
        String annot = getTriesAnnotation(curi);
        if (annot != null) {
            try {
                return Integer.parseInt(
                        annot.substring(TRIES_ANNOTATION_PREFIX.length()));
            } catch (NumberFormatException e) {
                LOGGER.warning("Could not parse annotation: " + annot);
            }
        }
        return 0;
    }

    /**
     * Pick up the launch ID from the ApplicationContext.
     * 
     * Requires appCtx be a PathSharingContext
     * 
     * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
     */
    public void setApplicationContext(ApplicationContext appCtx)
            throws BeansException {
        psc = (PathSharingContext) appCtx;
    }

    @Override
    public void onApplicationEvent(ApplicationEvent event) {
        if (event instanceof CrawlStateEvent) {
            this.launchId = psc.getCurrentLaunchId();
            LOGGER.info(
                    "Got lauchId: " + this.launchId + " from event " + event);
        }
    }
    
    /**
     * Create a prefix for the WARC file names, based on job name, launch id,
     * and current date.
     * 
     * {warcFilePrefix}-{job}-{launchId}
     * 
     * @return
     */
    private String buildWarcPrefix() {
        return warcFilePrefix + "-" + controller.getMetadata().getJobName()
                + "-" + launchId;
    }

    /**
     * Attempts to perform the rendering process.
     * 
     * Will retry a few times if this fails in an unexpected way.
     * 
     * @param url
     * @return True if it worked, false if not
     */
    private boolean doWrender(CrawlURI curi) {
        try {
            UriBuilder builder = UriBuilder.fromUri(getWrenderEndpoint())
                    .queryParam("url", curi.getURI());
            // Add warc_prefix based on launch ID
            String warcPrefix = this.buildWarcPrefix();
            builder = builder.queryParam("warc_prefix", warcPrefix);
            URL wrenderUrl = builder.build().toURL();
            // Read render result as JSON:
            JSONObject har = readJsonFromUrl(wrenderUrl, this.connectTimeout,
                    this.readTimeout);
            processHar(har, curi);

            // If we find status code, assume rendering worked:
            if (curi.getFetchStatus() != 0) {
                curi.addExtraInfo("warcPrefix", warcPrefix);
                // FetchStatusCodes.S_BLOCKED_BY_CUSTOM_PROCESSOR: ???
                return true;
            }
        } catch (Exception e) {
            LOGGER.log(Level.SEVERE, "Web rendering " + getWrenderEndpoint()
                    + " failed with unexpected exception: " + e, e);
        }
        // Either we hit an exception, or got a FetchStatus of 0, so:
        // Defer for retry:
        curi.setFetchStatus(FetchStatusCodes.S_DEFERRED);
        return false;
    }

    /**
     * 
     * Note that the status code must be set for outlinks to be enqueued.
     * @see org.archive.crawler.postprocessor.CandidatesProcessor.innerProcess(CrawlURI)
     * 
     * So we attempt to find it here. If this fails, we say the render failed (see above)
     * 
     * @param har
     * @param curi
     */
    protected static void processHar(JSONObject har, CrawlURI curi) {
        // Find the request for the curi:
        JSONArray entries = har.getJSONObject("log").getJSONArray("entries");
        for (int i = 0; i < entries.length(); i++) {
            JSONObject entry = entries.getJSONObject(i);
            // Turn string from HAR into a URI before comparison, to
            // compensate for escaped characters etc.:
            String url = entry.getJSONObject("request").getString("url");
            try {
                if (!url.startsWith("data:")) {
                    UURI requestUri = UURIFactory.getInstance(url);
                    if (curi.getUURI().equals(requestUri)) {
                        int status_code = entry.getJSONObject("response")
                                .getInt("status");
                        // Record that this worked so the rest of the Fetch
                        // Chain
                        // can be skipped:
                        curi.setFetchStatus(status_code);
                        // Extract status code:
                        curi.getAnnotations()
                                .add("WebRenderStatus:" + status_code);
                        // Only grab the first one:
                        break;
                    }
                } else {
                    LOGGER.fine("Skipping data: URI embedded in " + curi);
                }
            } catch (URIException e) {
                LOGGER.warning("Could not parse as UURI: " + url);
            }
        }
        
        // Add discovered outlinks to be enqueued:
        JSONArray pages = har.getJSONObject("log").getJSONArray("pages");
        for (int i = 0; i < pages.length(); i++) {
            JSONObject page = pages.getJSONObject(i);
            LOGGER.finest("JSON for PAGE " + page);
            // Get the content, if present:
            // JSONObject rc = page.getJSONObject("renderedContent");
            // String responseBody = rc.getString("text");
            // responseBody = Base64.base64Decode(responseBody);
            //
            // Not that easy to set up.
            //
            // ExtractorHTML uses
            // ReplayCharSequence cs =
            // curi.getRecorder().getContentReplayCharSequence();
            //
            // Process the link map:
            JSONArray map = page.getJSONArray("map");
            LOGGER.finest("JSON for MAP " + map);
            for (int j = 0; j < map.length(); j++) {
                JSONObject mapi = map.getJSONObject(j);
                // Not all map entries have a 'href' (e.g. 'onclick'):
                if (mapi.has("href")) {
                    Object href = mapi.get("href");
                    // Most hrefs are simple strings:
                    if (href instanceof String) {
                        String newUri = (String) href;
                        enqueueLink(newUri, curi);
                    }
                    // But some are dictionaries with URLs under 'animVal' and
                    // 'baseVal' keys.
                    // See https://github.com/ukwa/python-shepherd/issues/15
                    else if (href instanceof JSONObject) {
                        JSONObject jhref = (JSONObject) href;
                        for (String href_key : JSONObject.getNames(jhref)) {
                            enqueueLink(jhref.getString(href_key), curi);
                        }
                    }
                }
            }
        }
    }

    /**
     * 
     * @param newUri
     * @param curi
     */
    private static void enqueueLink(String newUri, CrawlURI curi) {
        // Do not enqueue 'data:' URIs:
        if (newUri.startsWith("data:")) {
            LOGGER.finest("Discarding data URI: " + newUri);
            return;
        }
        try {
            UURI dest = UURIFactory.getInstance(curi.getBaseURI(), newUri);
            CrawlURI link = curi.createCrawlURI(dest, LinkContext.NAVLINK_MISC,
                    Hop.NAVLINK);
            LOGGER.finer("Adding Outlink: " + link);
            curi.getOutLinks().add(link);
        } catch (URIException e) {
            LOGGER.log(Level.SEVERE, "URIException when processing " + newUri,
                    e);
        }
    }

    /**
     * Attempts to read a JSON result from the given URL.
     * 
     * Will time-out if there is no response.
     * 
     * @param url
     * @return
     * @throws IOException
     * @throws JSONException
     */
    protected static JSONObject readJsonFromUrl(URL url, int connectTimeout,
            int readTimeout)
            throws IOException, JSONException {
        URLConnection con = url.openConnection();
        con.setConnectTimeout(connectTimeout); // Time-out (e.g. 2m) for
                                              // connections in
                                              // case service is a bit busy.
        con.setReadTimeout(readTimeout); // Long (e.g. 5m) time-out for reads
                                           // as
                                           // rendering can be slow.
        InputStream is = con.getInputStream();
        try {
            BufferedReader rd = new BufferedReader(
                    new InputStreamReader(is, Charset.forName("UTF-8")));
            String jsonText = IOUtils.toString(rd);
            JSONObject json = new JSONObject(jsonText);
            return json;
        } finally {
            is.close();
        }
    }

}
