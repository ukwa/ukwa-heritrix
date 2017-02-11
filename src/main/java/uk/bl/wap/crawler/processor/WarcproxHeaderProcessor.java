/**
 * 
 */
package uk.bl.wap.crawler.processor;

import java.util.Map;
import java.util.logging.Logger;

import org.archive.crawler.event.CrawlStateEvent;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.archive.spring.PathSharingContext;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ApplicationEvent;
import org.springframework.context.ApplicationListener;

/**
 * 
 * This processor adds a header to the CrawlURI so that, when crawling via
 * warcprox, the crawl can control the naming of the warc files.
 * 
 * The same prefix information is also made available to the post-crawl logging
 * process. This helps ensure the crawl logs can be separated out in a way that
 * means they are consistent with the contents of the WARC files.
 * 
 * TODO It has some additional code to pick up the launch ID, but it's not clear
 * this is necessary as we can split on crawl date instead.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WarcproxHeaderProcessor extends Processor implements
        ApplicationContextAware, ApplicationListener<ApplicationEvent> {

    private PathSharingContext psc = null;
    private String launchId = null;

    private final static Logger LOGGER = Logger
            .getLogger(WarcproxHeaderProcessor.class.getName());

    /**
     * Pick up the launch ID from the ApplicationContext.
     * 
     * Requires appCtx be a PathSharingContext
     * 
     * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
     */
    @Override
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


    @Override
    protected boolean shouldProcess(CrawlURI curi) {
        String scheme = curi.getUURI().getScheme();
        if (!(scheme.equals("http") || scheme.equals("https"))) {
            // handles only plain http and https
            return false;
        }
        return true;
    }

    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        // Come up with a suitable header:
        String warcPrefix = "H3-2016";
        // Add a custom header to the URI object:
        @SuppressWarnings("unchecked")
        Map<String, String> uriCustomHeaders = (Map<String, String>) curi
                .getData().get("customHttpRequestHeaders");
        uriCustomHeaders.put("Warcprox-Meta",
                "{ \"warc-prefix\": \"H3-" + warcPrefix + "\" }");
        // Also add to the extra-info so it gets logged (and logging can split
        // on it):
        curi.addExtraInfo("warcPrefix", warcPrefix);
    }

}
