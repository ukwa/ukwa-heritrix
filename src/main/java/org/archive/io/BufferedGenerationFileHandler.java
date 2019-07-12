/**
 * 
 */
package org.archive.io;

import java.io.File;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.archive.util.FileUtils;

/**
 * 
 * Flushes the logs every X milliseconds and every Y lines.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class BufferedGenerationFileHandler extends GenerationFileHandler {

    /*
     * Honour constructors
     */

    public BufferedGenerationFileHandler(String pattern, boolean append,
            boolean shouldManifest)
    throws IOException, SecurityException {
        super(pattern, append, shouldManifest);
        timer.scheduleAtFixedRate(new FlushTimerTask(this), 0, maxBufferedTime);
    }

    public BufferedGenerationFileHandler(LinkedList<String> filenameSeries,
            boolean shouldManifest) throws IOException {
        super(filenameSeries, shouldManifest);
        timer.scheduleAtFixedRate(new FlushTimerTask(this), 0, maxBufferedTime);
    }

    private int maxBufferedTime = 2 * 1000; // Buffer for no more than two
                                            // seconds
    private int maxBufferedLines = 1000; // Buffer no more than one thousand
                                         // lines

    /**
     * @return the maxBufferedTime
     */
    public int getMaxBufferedTime() {
        return maxBufferedTime;
    }

    /**
     * @param maxBufferedTime
     *            the maxBufferedTime to set
     */
    public void setMaxBufferedTime(int maxBufferedTime) {
        this.maxBufferedTime = maxBufferedTime;
    }

    /**
     * @return the maxBufferedLines
     */
    public int getMaxBufferedLines() {
        return maxBufferedLines;
    }

    /**
     * @param maxBufferedLines
     *            the maxBufferedLines to set
     */
    public void setMaxBufferedLines(int maxBufferedLines) {
        this.maxBufferedLines = maxBufferedLines;
    }

    /** Helper to ensure flush is called at least once per second */

    private Timer timer = new Timer();

    public class FlushTimerTask extends TimerTask {

        private BufferedGenerationFileHandler gfh;

        public FlushTimerTask(BufferedGenerationFileHandler gfh) {
            this.gfh = gfh;
        }

        public void run() {
            gfh.reallyFlush();
        }

    }

    /**
     * Constructor-helper that rather than clobbering any existing file, moves
     * it aside with a timestamp suffix.
     * 
     * @param filename
     * @param append
     * @param shouldManifest
     * @return
     * @throws SecurityException
     * @throws IOException
     */
    public static BufferedGenerationFileHandler makeNew(String filename,
            boolean append,
            boolean shouldManifest) throws SecurityException, IOException {
        FileUtils.moveAsideIfExists(new File(filename));
        return new BufferedGenerationFileHandler(filename, append,
                shouldManifest);
    }


    /**
     * Flush only 1/Xth of the usual once-per-record, to reduce the time spent
     * holding the synchronization lock. (Flush is primarily called in a
     * superclass's synchronized publish()).
     * 
     * The eventual close calls a direct flush on the target writer, so all
     * rotates/ends will ultimately be fully flushed.
     * 
     * @see java.util.logging.StreamHandler#flush()
     */
    @Override
    public synchronized void flush() {
        if (flushCount.incrementAndGet() >= maxBufferedLines) {
            reallyFlush();
        }
    }

    /* Keep track of events */
    AtomicInteger flushCount = new AtomicInteger(0);

    /**
     * For actually flushing the actual log stream.
     */
    public synchronized void reallyFlush() {
        super.flush();
        flushCount.set(0);
    }

}
