package uk.bl.wap.crawler.processor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.ws.rs.core.UriBuilder;

import org.apache.commons.httpclient.URIException;
import org.apache.commons.io.IOUtils;
import org.archive.crawler.event.CrawlStateEvent;
import org.archive.modules.CrawlURI;
import org.archive.modules.ProcessResult;
import org.archive.modules.Processor;
import org.archive.modules.extractor.Hop;
import org.archive.modules.extractor.LinkContext;
import org.archive.net.UURI;
import org.archive.net.UURIFactory;
import org.archive.spring.PathSharingContext;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

/**
 * An alternative to FetchHTTP & ExtractHTML
 */
public class WrenderProcessor extends Processor implements
        ApplicationContextAware,
        ApplicationListener<ApplicationEvent> {

    private String wrenderEndpoint = "http://localhost:8000/render";

    private PathSharingContext psc = null;
    private String launchId = null;

    private final static Logger LOGGER = Logger
            .getLogger(WrenderProcessor.class.getName());

    public static final String HTTP_SCHEME = "http";
    public static final String HTTPS_SCHEME = "https";
    public static final String ANNOTATION = "WrenderedURL";

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
        // Attempt to render and extract links using a web-rendering service:
        boolean wrendered = this.doWrender(curi);
        // Did that work?
        if (wrendered) {
            // Wrendering worked, so halt this chain.
            return ProcessResult.FINISH;
        } else {
            // If that did't work, let the usual H3 process chain handle this
            // URL:
            return ProcessResult.PROCEED;
        }
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
     * Attempts to perform the rendering process.
     * 
     * Will retry a few times if this fails in an unexpected way.
     * 
     * @param url
     * @return True if it worked, false if not
     */
    private boolean doWrender(CrawlURI curi) {
        int tries = 0;
        while (tries < 3) {
            try {
                UriBuilder builder = UriBuilder.fromUri(getWrenderEndpoint())
                        .queryParam("url", curi.getURI());
                URL wrenderUrl = builder.build().toURL();
                JSONObject har = readJsonFromUrl(wrenderUrl);
                processHar(har, curi);
                return true;
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE,
                        "Web rendering failed with unexpected exception ", e);
                tries++;
            }
            try {
                Thread.sleep(1000 * 10);
            } catch (InterruptedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return false;
    }

    /**
     * 
     * @param har
     * @param curi
     */
    protected static void processHar(JSONObject har, CrawlURI curi) {
        // Annotate:
        curi.getAnnotations().add(ANNOTATION);
        // Find the request for the curi:
        JSONArray entries = har.getJSONObject("log").getJSONArray("entries");
        for (int i = 0; i < entries.length(); i++) {
            JSONObject entry = entries.getJSONObject(i);
            if (curi.getURI()
                    .equals(entry.getJSONObject("request").getString("url"))) {
                // Extract status code:
                curi.setFetchStatus(
                        entry.getJSONObject("response").getInt("status"));
            }
        }
        
        // Add discovered outlinks to be enqueued:
        JSONArray pages = har.getJSONObject("log").getJSONArray("pages");
        for (int i = 0; i < pages.length(); i++) {
            JSONObject page = pages.getJSONObject(i);
            JSONArray map = page.getJSONArray("map");
            for (int j = 0; j < map.length(); j++) {
                String newUri = map.getJSONObject(j).getString("href");
                try {
                    UURI dest = UURIFactory.getInstance(curi.getBaseURI(),
                            newUri);
                    CrawlURI link = curi.createCrawlURI(dest,
                            LinkContext.NAVLINK_MISC, Hop.NAVLINK);
                    curi.getOutLinks().add(link);
                } catch (URIException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * Attempts to read a JSON result from the given URL
     * 
     * @param url
     * @return
     * @throws IOException
     * @throws JSONException
     */
    protected static JSONObject readJsonFromUrl(URL url)
            throws IOException, JSONException {
        InputStream is = url.openStream();
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
