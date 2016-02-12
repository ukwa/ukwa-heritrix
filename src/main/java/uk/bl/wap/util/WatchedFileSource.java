/**
 * 
 */
package uk.bl.wap.util;

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public abstract class WatchedFileSource {

    private static Logger LOGGER = Logger
            .getLogger(WatchedFileSource.class.getName());

    private int checkInterval = 0;

    long lastUpdated = 0;

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
    private Thread updateThread = null;

    /**
     * load exclusion file and startup polling thread to check for updates
     * 
     * @throws IOException
     *             if the exclusion file could not be read.
     */
    public void init() {
        try {
            checkForReload();
        } catch (IOException e) {
            LOGGER.log(Level.SEVERE, "Exception when loading file!", e);
        }
        if (checkInterval > 0) {
            LOGGER.info("Starting update watcher thread for "
                    + this.getSourceFile());
            startUpdateThread();
        } else {
            LOGGER.info("Not starting watcher thread, checkInterval = "
                    + this.checkInterval);
        }
        
    }

    protected abstract File getSourceFile();

    protected abstract void loadFile() throws IOException;

    protected void checkForReload() throws IOException {
        File file = getSourceFile();
        if (file == null) {
            LOGGER.severe("No input file set.");
            return;
        }

        long currentMod = file.lastModified();
        if (currentMod == lastUpdated) {
            LOGGER.fine("File " + file.getName() + " appears unchanged.");
            if (currentMod == 0) {
                LOGGER.severe("No file at " + file.getAbsolutePath());
            }
            return;
        }

        if (file.length() == 0) {
            LOGGER.severe("File at " + file.getAbsolutePath() + " is empty!");
            return;
        }

        LOGGER.info("(Re)loading file " + file.getAbsolutePath());
        try {
            loadFile();
            lastUpdated = currentMod;
            LOGGER.info("Reload " + file.getAbsolutePath() + " OK");
        } catch (Exception e) {
            lastUpdated = -1;
            e.printStackTrace();
            LOGGER.severe("Reload " + file.getAbsolutePath() + " FAILED:"
                    + e.getLocalizedMessage());
        }
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
        LOGGER.info("stopUpdateThread down watcher on:" + this.getSourceFile());
        if (updateThread == null) {
            return;
        }
        updateThread.interrupt();
        new Exception("But WHY?").printStackTrace();
    }

    private class CacheUpdaterThread extends Thread {
        /**
         */
        private WatchedFileSource service = null;

        private int runInterval;

        /**
         * @param service
         *            ExclusionFactory which will be reloaded
         * @param runInterval
         *            int number of seconds between reloads
         */
        public CacheUpdaterThread(WatchedFileSource service, int runInterval) {
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
                        service.checkForReload();
                    } catch (IOException e) {
                        LOGGER.log(Level.SEVERE,
                                "Exception while checking for reload: ", e);
                    }
                    Thread.sleep(sleepInterval * 1000);
                } catch (InterruptedException e) {
                    LOGGER.log(Level.SEVERE, "Exception when reloading!", e);
                    e.printStackTrace();
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
        LOGGER.info("Shutting down watcher on:" + this.getSourceFile());
        stopUpdateThread();
    }
}
