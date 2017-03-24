/**
 * 
 */
package uk.bl.wap.modules.uriuniqfilters;

import java.io.File;
import java.util.logging.Logger;

import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.springframework.beans.factory.annotation.Required;

/**
 * 
 * Note that this version uses the TTL on check, having stored the PUT time,
 * rather than storing the EXPIRE time (as other implementations do).
 * 
 * Note default lockScale for MapDB is 16. This defaults to 1024, which will
 * consume more RAM. It can be configured via the crawler beans file.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class MapDBRecentlySeenUriUniqFilter extends RecentlySeenUriUniqFilter {

    /**  */
    private static final long serialVersionUID = 7016514645624582438L;

    private static Logger LOGGER = Logger
            .getLogger(MapDBRecentlySeenUriUniqFilter.class.getName());

    private String cacheStorePath = System.getProperty("java.io.tmpdir");

    private DB db;

    private HTreeMap<String, Long> cache;

    private int lockScale = 1024;

    private long sizeCounter = 0;

    private long flushFrequency = 2;

    /**
     * 
     * @return
     */
    public String getCacheStorePath() {
        return cacheStorePath;
    }

    /**
     * 
     * @param cacheStorePath
     */
    @Required
    public void setCacheStorePath(String cacheStorePath) {
        this.cacheStorePath = cacheStorePath;
    }

    /**
     * @return the lockScale
     */
    public int getLockScale() {
        return lockScale;
    }

    /**
     * @param lockScale
     *            the lockScale to set
     */
    public void setLockScale(int lockScale) {
        this.lockScale = lockScale;
    }

    /* (non-Javadoc)
     * @see uk.bl.wap.util.RecentlySeenUriUniqFilter#setAddWithTTL(java.lang.String, java.lang.String, int)
     */
    @Override
    public boolean setAddWithTTL(String key, String uri, int ttl_s) {
        long currentTime = System.currentTimeMillis() / 1000;
        LOGGER.finest("Checking cache for " + key + " -> " + currentTime
                + " TTL " + ttl_s);
        Long oldValue = this.cache.putIfAbsent(key, currentTime);
        if (oldValue == null) {
            LOGGER.finest("New URL - stored " + key + " -> " + currentTime);
            this.sizeCounter++;
            // Periodically flush (to control RAM usage):
            if (this.sizeCounter % flushFrequency == 0) {
                LOGGER.fine("Flushing URI cache...");
                this.db.commit();
                LOGGER.fine("Flushed URI cache.");
            }
            return true;
        }
        LOGGER.finest("Seen URL - stored value is " + key + " -> " + oldValue);
        // Has the entry expired?
        if ((currentTime - oldValue) > ttl_s) {
            LOGGER.finest("Seen but expired: " + key);
            LOGGER.finest("Delta " + (currentTime - oldValue) + " > " + ttl_s);
            this.cache.put(key, currentTime);
            return true;
        }
        // Otherwise, cached and not due to expire, so return false:
        LOGGER.finest("Seen URL: " + key);
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setRemove(java.lang.CharSequence)
     */
    @Override
    protected boolean setRemove(CharSequence key) {
        Long oldValue = this.cache.remove(key.toString());
        if (oldValue == null) {
            return false;
        }
        return true;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setCount()
     */
    @Override
    protected long setCount() {
        if (this.cache != null) {
            // This is accurate but actually recounts! and so is v expensive!
            // return this.cache.sizeLong();
            return this.sizeCounter;
        }
        return 0;
    }


    @Override
    public long requestFlush() {
        this.db.commit();
        return 0;
    }

    @Override
    public void close() {
        this.closeCache();
        super.close();
    }

    @Override
    protected void finalize() throws Throwable {
        this.closeCache();
        super.finalize();
    }

    @Override
    public void start() {
        super.start();
        LOGGER.info("Called start()");
        this.setupCache();
    }

    @Override
    public void stop() {
        super.stop();
        LOGGER.info("Called stop()");
        this.closeCache();
    }

    @Override
    public boolean isRunning() {
        LOGGER.info("Called isRunning()");
        if (this.db != null) {
            return !this.db.isClosed();
        }
        return false;
    }

    /**
     * 
     */
    private void setupCache() {
        // Create a the cache folder if needed:
        File cacheFolder = new File(cacheStorePath);
        if (!cacheFolder.exists()) {
            cacheFolder.mkdirs();
        }
        // Create the DB:
        db = DBMaker.fileDB(new File(cacheFolder, "recentlySeen.db"))
                .closeOnJvmShutdown().lockScale(lockScale).metricsEnable()
                .cacheSoftRefEnable().checksumEnable().compressionEnable()
                .fileMmapEnable().make();
        // Set up the cache:
        if (db.exists("onDiskUriCache")) {
            cache = db.hashMap("onDiskUriCache");
            LOGGER.info("Cache size count begun...");
            this.sizeCounter = this.cache.sizeLong();
            LOGGER.info("Cache size count finished: " + this.sizeCounter);
        } else {
            cache = db.hashMapCreate("onDiskUriCache")
                .keySerializer(Serializer.STRING)
                .valueSerializer(Serializer.LONG).make();
        }
    }

    private void closeCache() {
        cache.close();
        db.commit();
        db.close();
    }

}
