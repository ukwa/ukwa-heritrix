/**
 * 
 */
package uk.bl.wap.util;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.commons.httpclient.URIException;
import org.archive.util.iterator.CloseableIterator;
import org.archive.wayback.UrlCanonicalizer;
import org.archive.wayback.surt.SURTTokenizer;
import org.archive.wayback.util.flatfile.FlatFile;
import org.archive.wayback.util.url.AggressiveUrlCanonicalizer;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WatchedFileSurtMap<T> {
    private static final Logger LOGGER = Logger
            .getLogger(WatchedFileSurtMap.class.getName());

    private int checkInterval = 0;
    private Map<String, Integer> currentMap = null;
    private File file = null;

    long lastUpdated = 0;

    private UrlCanonicalizer canonicalizer = new AggressiveUrlCanonicalizer();

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
     * @return the path
     */
    public String getFile() {
        return file.getAbsolutePath();
    }

    /**
     * @param path
     *            the file to set
     */
    public void setFile(String path) {
        this.file = new File(path);
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
    public void init() throws IOException {
        reloadFile();
        if (checkInterval > 0) {
            startUpdateThread();
        }
    }

    protected void reloadFile() throws IOException {
        if (file == null)
            return;

        long currentMod = file.lastModified();
        if (currentMod == lastUpdated) {
            if (currentMod == 0) {
                LOGGER.severe("No file at " + file.getAbsolutePath());
            }
            return;
        }
        LOGGER.info("Reloading file " + file.getAbsolutePath());
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

            String[] parts = line.split(" ");
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

            LOGGER.fine("EXCLUSION-MAP: adding " + surt + " " + parts[1]);
            newMap.put(surt, Integer.parseInt(parts[1]));
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
        private WatchedFileSurtMap<T> service = null;

        private int runInterval;

        /**
         * @param service
         *            ExclusionFactory which will be reloaded
         * @param runInterval
         *            int number of seconds between reloads
         */
        public CacheUpdaterThread(WatchedFileSurtMap<T> service,
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
