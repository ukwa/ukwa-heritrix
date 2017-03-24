package uk.bl.wap.modules.uriuniqfilters;

import java.io.Serializable;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.crawler.util.SetBasedUriUniqFilter;
import org.archive.spring.ConfigFile;
import org.archive.spring.ConfigPathConfigurer;
import org.archive.surt.SURTTokenizer;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Required;
import org.springframework.context.Lifecycle;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import uk.bl.wap.util.WatchedFileSurtMap;

/**
 * 
 * This UriUniqFilter or 'Already Seen URIs' filter uses an off-the-shelf cache
 * engine that supports time-to-live caching. i.e. you can set the cached
 * entries to expire, so the crawler will 'forget' it has seen the URI and allow
 * it to be re-queued for crawling.
 * 
 * This can be used to support regular crawling at arbitrary times of day, with
 * SURT prefixes used to map URLs to TTL. Therefore the same crawl can contain
 * URLs that are visited at varying frequencies.
 * 
 * It converts the URL to a large hash and uses that as the key, thus supporting
 * systems that place limits on the size of the key. The 128-bit keys should
 * look like this:
 * 
 * 6c1b07bc7bbc4be347939ac4a93c437a
 * 
 * TODO Consider adding 'Opposite of a Bloom filter' behaviour on top of the
 * caches. This would involve truncating the key fairly heavily, so there cannot
 * be more than, say 10 billion keys, in order to keep memory usage down. The
 * trick would then be to log hash collisions and allow the crawl to proceed if
 * different URLs have the same key.
 * 
 * @see http://www.somethingsimilar.com/2012/05/21/the-opposite-of-a-bloom-
 *      filter/
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public abstract class RecentlySeenUriUniqFilter extends SetBasedUriUniqFilter
        implements Serializable, Lifecycle, InitializingBean {
    private static final long serialVersionUID = 1061526253773091309L;

    private static Logger LOGGER = Logger
            .getLogger(RecentlySeenUriUniqFilter.class.getName());

    public static final int HOUR = 60 * 60;
    public static final int DAY = HOUR * 24;
    public static final int WEEK = DAY * 7;
    public int defaultTTL = 52 * WEEK;


    private ConfigPathConfigurer configPathConfigurer = null;

    private ConfigFile textSource = null;

    protected WatchedFileSurtMap ttlMap = new WatchedFileSurtMap(this);

    // Hash function used for building keys:
    private HashFunction hf = Hashing.murmur3_128();

    // Whether to use the hash of the URI rather than the URI (e.g. if
    // the implementation needs short keys)
    private boolean useHashedUriKey = false;

    /**
     * Default constructor
     */
    public RecentlySeenUriUniqFilter() {
        super();
    }

    /**
     * @return the configPathConfigurer
     */
    public ConfigPathConfigurer getConfigPathConfigurer() {
        return configPathConfigurer;
    }

    /**
     * @param configPathConfigurer
     *            the configPathConfigurer to set
     */
    @Autowired
    public void setConfigPathConfigurer(
            ConfigPathConfigurer configPathConfigurer) {
        this.configPathConfigurer = configPathConfigurer;
    }

    public ConfigFile getTextSource() {
        return textSource;
    }

    @Required
    public void setTextSource(ConfigFile textSource) {
        this.textSource = textSource;
        // Seem to need to plumb this in myself:
        if (this.getConfigPathConfigurer() != null) {
            this.textSource.setConfigurer(getConfigPathConfigurer());
            // And this is needed for it to work:
            this.textSource.setBase(this.getConfigPathConfigurer().getPath());
        }
        this.ttlMap.shutdown();
        this.ttlMap.init();
    }

    /**
     * @return the defaultTTL
     */
    public int getDefaultTTL() {
        return defaultTTL;
    }

    /**
     * @param defaultTTL
     *            the defaultTTL to set
     */
    public void setDefaultTTL(int defaultTTL) {
        this.defaultTTL = defaultTTL;
    }

    /**
     * @return the sourceCheckInterval
     */
    public int getSourceCheckInterval() {
        return this.ttlMap.getCheckInterval();
    }

    /**
     * @param sourceCheckInterval
     *            the sourceCheckInterval to set
     */
    public void setSourceCheckInterval(int sourceCheckInterval) {
        this.ttlMap.setCheckInterval(sourceCheckInterval);
    }

    /**
     * @return the useHashedUriKey
     */
    public boolean isUseHashedUriKey() {
        return useHashedUriKey;
    }

    /**
     * @param useHashedUriKey
     *            the useHashedUriKey to set
     */
    public void setUseHashedUriKey(boolean useHashedUriKey) {
        this.useHashedUriKey = useHashedUriKey;
    }

    /**
     * 
     * Use the map to look up the TTL for this Url.
     * 
     * @param url
     * @return TTL (in seconds)
     */
    private Integer getTTLForUrl(String url) {
        try {
            SURTTokenizer st = new SURTTokenizer(url);
            while (true) {
                String nextSearch = st.nextSearch();
                if (nextSearch == null) {
                    break;
                }
                LOGGER.finest("TTL-MAP:Checking " + nextSearch);
                if (ttlMap.get().containsKey(nextSearch)) {
                    Integer ttl = ttlMap.get().get(nextSearch);
                    LOGGER.fine("TTL-MAP: \"" + ttl + "s\" (" + url + ")");
                    return ttl;
                }
            }
        } catch (URIException e) {
            LOGGER.warning(e.toString());
        }
        return this.defaultTTL;
    }

    /**
     * 
     */
    protected boolean setAdd(CharSequence uri_cs) {
        String uri = uri_cs.toString();
        String key;
        if (useHashedUriKey) {
            key = hf.hashBytes(uri.getBytes()).toString();
        } else {
            key = uri;
        }
        int ttl_s = getTTLForUrl(uri);
        // Allow entries to expire after a while, defaults, ranges, etc,
        // surt-prefixed.
        return setAddWithTTL(key, uri, ttl_s);
    }

    /**
     * 
     * @param key
     * @param uri
     * @param ttl_s
     * @return true if the item is new and is being added to the set of known
     *         URIs
     */
    abstract public boolean setAddWithTTL(String key, String uri, int ttl_s);


    @Override
    public void start() {
        LOGGER.info("Called start()");
        this.ttlMap.init();
    }

    @Override
    public void stop() {
        LOGGER.info("Called stop()");
        this.ttlMap.shutdown();
    }

    @Override
    public boolean isRunning() {
        LOGGER.info("Called isRunning()");
        return this.ttlMap.isRunning();
    }

    /*
     * (non-Javadoc)
     * 
     * @see
     * org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        this.ttlMap.init();
    }

}
