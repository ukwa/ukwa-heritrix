package uk.bl.wap.util;

import java.io.Serializable;
import java.util.logging.Logger;

import org.archive.crawler.util.SetBasedUriUniqFilter;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Required;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;
import net.sf.ehcache.Element;
import net.sf.ehcache.Status;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.DiskStoreConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

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
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class RecentlySeenUriUniqFilter extends SetBasedUriUniqFilter
        implements Serializable, InitializingBean {
    private static final long serialVersionUID = 1061526253773091309L;

    private static Logger LOGGER = Logger
            .getLogger(RecentlySeenUriUniqFilter.class.getName());

    public static final int HOUR = 60 * 60;
    public static final int DAY = HOUR * 24;
    public static final int WEEK = DAY * 7;
    public static final int DEFAULT_TTL = 4 * WEEK;

    private String cacheStorePath;
    private CacheManager manager;
    private Ehcache cache;

    private int maxEntriesLocalHeap = 1000 * 1000;

    public String getCacheStorePath() {
        return cacheStorePath;
    }

    @Required
    public void setCacheStorePath(String cacheStorePath) {
        this.cacheStorePath = cacheStorePath;
    }

    public Ehcache getCache() {
        if (cache == null || !cache.getStatus().equals(Status.STATUS_ALIVE)) {
            setupCache();
        }
        return cache;
    }

    public void setCache(Ehcache cache) {
        this.cache = cache;
    }

    /**
     * Default constructor
     */
    public RecentlySeenUriUniqFilter() {
        super();
    }

    /**
     * Initializer.
     */
    public void afterPropertiesSet() {
        this.setupCache();
    }

    private void setupCache() {
        LOGGER.info("Setting up cache...");
        if (manager == null
                || !manager.getStatus().equals(Status.STATUS_ALIVE)) {
            Configuration configuration = new Configuration();
            DiskStoreConfiguration diskStoreConfiguration = new DiskStoreConfiguration();
            diskStoreConfiguration.setPath(cacheStorePath);
            // Already created a configuration object ...
            configuration.addDiskStore(diskStoreConfiguration);
            manager = CacheManager.create(configuration);
        }
        // Get existing cache if there is one:
        cache = manager.getEhcache("recentlySeenUrls");
        // Otherwise, make a new one:
        if (cache == null) {
            LOGGER.info("Setting up default Ehcache configuration.");
            cache = new Cache(new CacheConfiguration("recentlySeenUrls",
                    maxEntriesLocalHeap).memoryStoreEvictionPolicy(
                                    MemoryStoreEvictionPolicy.LFU)
                            .eternal(false).timeToLiveSeconds(DEFAULT_TTL)
                            .diskExpiryThreadIntervalSeconds(0)
                            .diskPersistent(true).overflowToDisk(true));
            manager.addCache(cache);
        }
    }

    /**
     * 
     */
    protected boolean setAdd(CharSequence uri) {
        // Build the cache entry:
        Element element = new Element(uri, uri);

        // Allow entries to expire after a while, defaults, ranges, etc,
        // surt-prefixed.
        element.setTimeToLive(60);

        // Add to the cache, if absent:
        Element added = getCache().putIfAbsent(element);
        if (added == null) {
            LOGGER.finest("Cache entry " + uri + " is new.");
        } else {
            LOGGER.finest("Cache entry " + uri + " is already in the cache.");
        }

        return (added == null);
    }

    /**
     * 
     */
    protected boolean setRemove(CharSequence uri) {
        return getCache().remove(uri);
    }

    /**
     * 
     */
    protected long setCount() {
        return getCache().getSize();
    }

    @Override
    public long requestFlush() {
        this.getCache().flush();
        return 0;
    }

    private void closeEhcache() {
        LOGGER.info("Shutting down the cache...");
        if (this.cache != null) {
            this.cache.flush();
        }
        if (this.manager != null
                && this.manager.getStatus().equals(Status.STATUS_ALIVE)) {
            this.manager.shutdown();
        }
    }

    @Override
    public void close() {
        this.closeEhcache();
        super.close();
    }

    @Override
    protected void finalize() throws Throwable {
        this.closeEhcache();
        super.finalize();
    }

}
