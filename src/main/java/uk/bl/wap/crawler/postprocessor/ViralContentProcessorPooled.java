package uk.bl.wap.crawler.postprocessor;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.commons.pool.ObjectPool;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.archive.io.ReplayInputStream;
import org.archive.modules.CrawlURI;
import org.archive.modules.Processor;
import org.springframework.beans.factory.annotation.Autowired;

import uk.bl.wap.util.ClamdScanner;

/**
 * 
 * Not convinced this is working as intended in H3.
 * 
 * Moreover, using a shared pool builds in more complexity and should not really
 * be necessary.
 * 
 * @author rcoram, Andrew Jackson
 */

public class ViralContentProcessorPooled extends Processor {
    private final static Logger LOGGER = Logger
            .getLogger(ViralContentProcessorPooled.class.getName());
    private static final int DEFAULT_MAX_SCANNERS = 100;
    private static final long MAX_WAIT = -1L;
    private int poolSize = DEFAULT_MAX_SCANNERS;
    private int virusCount = 0;
    private ClamdScannerPoolFactory clamdScannerPoolFactory = new ClamdScannerPoolFactory();
    private ObjectPool<ClamdScanner> clamdScannerPool = null;

    public ViralContentProcessorPooled() {
        initializePool();
    }

    private void initializePool() {
        if (this.clamdScannerPool != null) {
            // Check if this is necessary (i.e. if the size of the pool is
            // changing.
            // Using a sync to avoid idle/active status changing between reads.
            int currentSize = 0;
            synchronized (this.clamdScannerPool) {
                currentSize += this.clamdScannerPool.getNumIdle();
                currentSize += this.clamdScannerPool.getNumActive();
            }
            if (currentSize == this.poolSize) {
                return;
            }
            // Otherwise, shutdown ready to re-create:
            try {
                this.clamdScannerPool.close();
            } catch (Exception e) {
                LOGGER.log(Level.SEVERE, "Exception when closing pool.", e);
            }
            this.clamdScannerPool = null;
        }
        clamdScannerPool = new GenericObjectPool<ClamdScanner>(
                clamdScannerPoolFactory, poolSize,
                GenericObjectPool.WHEN_EXHAUSTED_BLOCK, MAX_WAIT);
    }

    /**
     * The host machine on which clamd is running.
     */
    @Autowired
    public void setClamdHost(String clamdHost) {
        kp.put("clamdHost", clamdHost);
        clamdScannerPoolFactory.setClamdHost(clamdHost);
    }

    public String getClamdHost() {
        return (String) kp.get("clamdHost");
    }

    /**
     * The port on which the instance of clamd can be found.
     */
    public int getClamdPort() {
        return (Integer) kp.get("clamdPort");
    }

    @Autowired
    public void setClamdPort(int port) {
        kp.put("clamdPort", port);
        clamdScannerPoolFactory.setClamdPort(port);
    }

    /**
     * The timeout in milliseconds for clamd.
     */
    @Autowired
    public void setClamdTimeout(int clamdTimeout) {
        kp.put("clamdTimeout", clamdTimeout);
        clamdScannerPoolFactory.setClamdTimeout(clamdTimeout);
    }

    public int getClamdTimeout() {
        return (Integer) kp.get("clamdTimeout");
    }

    @Autowired
    public void setStreamMaxLength(int streamMaxLength) {
        kp.put("streamMaxLength", streamMaxLength);
        clamdScannerPoolFactory.setStreamMaxLength(streamMaxLength);
    }

    public int getStreamMaxLength() {
        return (Integer) kp.get("streamMaxLength");
    }

    /**
     * @return the poolSize
     */
    public int getPoolSize() {
        return poolSize;
    }

    /**
     * @param poolSize
     *            the poolSize to set
     */
    public void setPoolSize(int poolSize) {
        this.poolSize = poolSize;
        initializePool();
    }

    @Override
    protected void innerProcess(CrawlURI curi) throws InterruptedException {
        ClamdScanner scanner = null;
        ReplayInputStream in = null;
        try {
            LOGGER.log(Level.FINER, "ClamAV scanning " + curi.getURI());
            scanner = clamdScannerPool.borrowObject();
            in = curi.getRecorder().getReplayInputStream();
            String result = scanner.clamdScan(in);
            LOGGER.log(Level.FINE, "ClamAV scanned " + curi.getURI()
                    + " got result: " + result);
            if (result.matches("^([1-2]:\\s+)?stream:.+$")) {
                if (!result.matches("^([1-2]:\\s+)?stream: OK.*$")) {
                    curi.getAnnotations().add(result);
                    virusCount++;
                }
            } else {
                LOGGER.log(Level.WARNING, "Invalid ClamAV response: " + result);
            }
        } catch (Throwable e) {
            LOGGER.log(Level.WARNING, "innerProcess(): " + e.toString());
        } finally {
            try {
                if (in != null)
                    in.close();
            } catch (IOException e) {
                LOGGER.log(Level.WARNING, "Exception when closing ReplayInputStream. ", e);
            }
        }
        if (scanner != null) {
            try {
                clamdScannerPool.returnObject(scanner);
            } catch (Exception e) {
                LOGGER.log(Level.WARNING, "innerProcess(): " + e.toString());
            }
        }
    }

    @Override
    protected boolean shouldProcess(CrawlURI uri) {
        return uri.is2XXSuccess() && (uri.getContentLength() > 0L);
    }

    @Override
    public String report() {
        StringBuffer report = new StringBuffer();
        report.append(super.report());
        report.append("  Streams scanned: " + this.getURICount() + "\n");
        report.append("  Viruses found:   " + this.virusCount + "\n");

        return report.toString();
    }

    public class ClamdScannerPoolFactory
            extends BasePoolableObjectFactory<ClamdScanner> {
        String clamdHost;
        Integer clamdPort;
        Integer clamdTimeout;
        Integer streamMaxLength;

        @Override
        public ClamdScanner makeObject() throws Exception {
            return new ClamdScanner(this.getClamdHost(), this.getClamdPort(),
                    this.getClamdTimeout(), this.getStreamMaxLength());
        }

        public void setClamdHost(String clamdHost) {
            this.clamdHost = clamdHost;
        }

        private String getClamdHost() {
            return this.clamdHost;
        }

        public int getClamdPort() {
            return this.clamdPort;
        }

        public void setClamdPort(int clamdPort) {
            this.clamdPort = clamdPort;
        }

        public void setClamdTimeout(int clamdTimeout) {
            this.clamdTimeout = clamdTimeout;
        }

        private int getClamdTimeout() {
            return this.clamdTimeout;
        }

        public void setStreamMaxLength(int streamMaxLength) {
            this.streamMaxLength = streamMaxLength;
        }

        private int getStreamMaxLength() {
            return streamMaxLength;
        }
    }
}
