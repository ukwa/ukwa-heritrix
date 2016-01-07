/**
 * 
 */
package uk.bl.wap.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.surt.SURTTokenizer;
import org.archive.wayback.util.flatfile.FlatFile;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

/**
 * 
 * Based on OpenWayback StaticMapExclusionFilterFactory.
 * 
 * TODO Only supports URLs right now - should copy SurtPrefixSet and use a + to
 * add surts directly, like in the surts.txt file and SurtPrefixedDecideRule.
 * 
 * @see https://github.com/iipc/webarchive-commons/blob/master/src/main/java/org
 *      /archive/util/SurtPrefixSet.java#L120
 *      SurtPrefixSet.importFromMixed(Reader r, boolean deduceFromSeeds)
 * 
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WatchedFileSurtMap {

    private static final Logger LOGGER = Logger
            .getLogger(WatchedFileSurtMap.class.getName());

    private int checkInterval = 0;

    private Map<String, Integer> currentMap = null;

    long lastUpdated = 0;

    private UrlCanonicalizer canonicalizer = new AggressiveUrlCanonicalizer();

    private RecentlySeenUriUniqFilter recentlySeenUriUniqFilter;

    public WatchedFileSurtMap(
            RecentlySeenUriUniqFilter recentlySeenUriUniqFilter) {
        this.recentlySeenUriUniqFilter = recentlySeenUriUniqFilter;
    }

    public UrlCanonicalizer getCanonicalizer() {
        return canonicalizer;
    }

    public void setCanonicalizer(UrlCanonicalizer canonicalizer) {
        this.canonicalizer = canonicalizer;
    }

    /**
     * @return the checkInterval in seconds
     */
    public int getCheckInterval() {
        return checkInterval;
    }

    /**
     * @param checkInterval
     *            the checkInterval in seconds to set
     */
    public void setCheckInterval(int checkInterval) {
        this.checkInterval = checkInterval;
    }

    /**
     * Thread object of update thread -- also is flag indicating if the thread
     * has already been started -- static, and access to it is synchronized.
     */
    private static Thread updateThread = null;

    /**
     * load exclusion file and startup polling thread to check for updates
     * 
     * @throws IOException
     *             if the exclusion file could not be read.
     */
    public void init() {
        if (this.recentlySeenUriUniqFilter.getTextSource() != null) {
            LOGGER.warning(
                    "Initialising TTL MAP: " + this.recentlySeenUriUniqFilter
                .getTextSource().getFile().getAbsolutePath());
        } else {
            LOGGER.severe("No TTL Map text source defined!");
        }
        try {
            reloadFile();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception when loading file!", e);
        }
        if (checkInterval > 0) {
            startUpdateThread();
        }
    }

    protected void reloadFile() throws IOException {
        if (this.recentlySeenUriUniqFilter == null
                || this.recentlySeenUriUniqFilter.getTextSource() == null)
            return;

        File file = this.recentlySeenUriUniqFilter.getTextSource().getFile();

        long currentMod = file.lastModified();
        if (currentMod == lastUpdated) {
            if (currentMod == 0) {
                LOGGER.severe("No file at " + file.getAbsolutePath());
            }
            return;
        }
        LOGGER.info("(Re)loading file " + file.getAbsolutePath());
        try {
            currentMap = loadFile(file.getAbsolutePath());
            lastUpdated = currentMod;
            LOGGER.info("Reload " + file.getAbsolutePath() + " OK");
        } catch (IOException e) {
            lastUpdated = -1;
            currentMap = null;
            e.printStackTrace();
            LOGGER.severe("Reload " + file.getAbsolutePath() + " FAILED:"
                    + e.getLocalizedMessage());
        }
    }

    protected Map<String, Integer> loadFile(String path) throws IOException {
        Map<String, Integer> newMap = new HashMap<String, Integer>();
        FlatFile ff = new FlatFile(path);
        CloseableIterator<String> itr = ff.getSequentialIterator();
        LOGGER.fine("EXCLUSION-MAP: looking at " + itr.hasNext());
        while (itr.hasNext()) {
            String line = (String) itr.next();
            line = line.trim();
            LOGGER.fine("EXCLUSION-MAP: looking at " + line);

            if (line.length() == 0) {
                continue;
            }

            String[] parts = line.split(" ", 2);
            String key = parts[0];
            try {
                key = canonicalizer.urlStringToKey(key);
            } catch (URIException exc) {
                LOGGER.finest("Exception when parsing: " + exc);
                continue;
            }

            String surt;

            if (canonicalizer.isSurtForm()) {
                surt = key;
            } else {
                surt = key.startsWith("(") ? key : SURTTokenizer.prefixKey(key);
            }

            Integer ps = Integer.parseInt(parts[1]);
            LOGGER.fine("EXCLUSION-MAP: adding " + surt + " " + ps + "s");
            newMap.put(surt, ps);
        }
        itr.close();
        return newMap;
    }

    /**
     * @return ObjectFilter which blocks CaptureSearchResults in the exclusion
     *         file.
     */
    public Map<String, Integer> get() {
        if (currentMap == null) {
            return new HashMap<String, Integer>();
        }
        return currentMap;
    }

    private synchronized void startUpdateThread() {
        if (updateThread != null) {
            return;
        }
        updateThread = new CacheUpdaterThread(this, checkInterval);
        updateThread.start();
    }

    public synchronized boolean isRunning() {
        if (updateThread == null) {
            return false;
        } else {
            return true;
        }
    }

    private synchronized void stopUpdateThread() {
        if (updateThread == null) {
            return;
        }
        updateThread.interrupt();
    }

    private class CacheUpdaterThread extends Thread {
        /**
         * object which merges CDX files with the BDBResourceIndex
         */
        private WatchedFileSurtMap service = null;

        private int runInterval;

        /**
         * @param service
         *            ExclusionFactory which will be reloaded
         * @param runInterval
         *            int number of seconds between reloads
         */
        public CacheUpdaterThread(WatchedFileSurtMap service,
                int runInterval) {
            super("CacheUpdaterThread");
            super.setDaemon(true);
            this.service = service;
            this.runInterval = runInterval;
            LOGGER.info("CacheUpdaterThread is alive.");
        }

        public void run() {
            int sleepInterval = runInterval;
            while (true) {
                try {
                    try {
                        service.reloadFile();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    Thread.sleep(sleepInterval * 1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    return;
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.wayback.accesscontrol.ExclusionFilterFactory#shutdown()
     */
    public void shutdown() {
        stopUpdateThread();
    }

}
