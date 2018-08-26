/**
 * 
 */
package uk.bl.wap.modules.deciderules;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Reader;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.commons.io.IOUtils;
import org.archive.io.ReadSource;
import org.archive.modules.CrawlURI;
import org.archive.modules.deciderules.surt.SurtPrefixedDecideRule;
import org.archive.spring.ConfigFile;
import org.archive.spring.ConfigPathConfigurer;
import org.archive.util.SurtPrefixSet;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;

import uk.bl.wap.util.WatchedFileSource;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class WatchedFileSurtPrefixedDecideRule extends SurtPrefixedDecideRule
        implements InitializingBean {

    private static final Logger logger = Logger
            .getLogger(WatchedFileSurtPrefixedDecideRule.class.getName());
    /** */
    private static final long serialVersionUID = 5612559755412596566L;

    private class WatchedSurtFile extends WatchedFileSource {

        private WatchedSurtFile() {
        }

        @Override
        protected File getSourceFile() {
            ReadSource source = getSurtsSource();
            if( source instanceof ConfigFile) {
                return ((ConfigFile) source).getFile();
            } else {
                logger.severe(
                        "Cannot Watch unless the source is a ConfigFile : "
                                + source);
            }
            return null;
        }

        @Override
        protected void loadFile() throws IOException {
            if (getSurtsSource() != null) {
                if (logger.isLoggable(Level.FINE)) {
                    logger.fine(
                            "reading surt prefixes from " + getSurtsSource());
                }
                Reader reader = getSurtsSource().obtainReader();
                SurtPrefixSet newSurtPrefixes = new SurtPrefixSet();
                try {
                    newSurtPrefixes.importFromMixed(reader, true);
                } finally {
                    IOUtils.closeQuietly(reader);
                }
                // Ignore empty files (assume this is in error):
                if (newSurtPrefixes.size() == 0) {
                    logger.severe(
                            "No SURTs found! Assuming this is in error - not modifying current SURTs");
                } else {
                    // And swap into place:
                    synchronized (surtPrefixes) {
                        surtPrefixes = newSurtPrefixes;
                    }
                }
                // Log current surt prefixes (for debugging):
                for (String s : surtPrefixes) {
                    logger.fine("SURT: " + s);
                }
            }
        }

    }

    private WatchedSurtFile watchSurtFile;
    private ConfigPathConfigurer configPathConfigurer;

    public WatchedFileSurtPrefixedDecideRule() {
        this.watchSurtFile = new WatchedSurtFile();
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

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.modules.deciderules.surt.SurtPrefixedDecideRule#
     * setSurtsSourceFile(org.archive.spring.ConfigFile)
     */
    @Override
    public void setSurtsSourceFile(ConfigFile cp) {
        logger.info("Setting SURT source to " + cp.getPath());
        // Seem to need to plumb this in myself:
        if (this.getConfigPathConfigurer() != null) {
            cp.setConfigurer(getConfigPathConfigurer());
            // And this is needed for it to work:
            cp.setBase(this.getConfigPathConfigurer().getPath());
        }
        super.setSurtsSource(cp);
    }

    /**
     * @return the sourceCheckInterval
     */
    public int getSourceCheckInterval() {
        return this.watchSurtFile.getCheckInterval();
    }

    /**
     * @param sourceCheckInterval
     *            the sourceCheckInterval to set
     */
    public void setSourceCheckInterval(int sourceCheckInterval) {
        logger.info("Setting sourceCheckInterval to " + sourceCheckInterval);
        this.watchSurtFile.setCheckInterval(sourceCheckInterval);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        this.watchSurtFile.init();
    }

    @Override
    public void addedSeed(CrawlURI curi) {
        // If enabled, add new seeds to the SURT scope:
        if(getSeedsAsSurtPrefixes()) {
            String prefix = prefixFrom(curi.getURI());
            boolean added = surtPrefixes.add(prefix);
            // If this is new, append it to the surts file:
            if (added) {
                // One thread at a time, please:
                synchronized (this) {
                    // Append, using a FileLock to attempt to ensure
                    // consistency:
                    File sourceFile = this.watchSurtFile.getSourceFile();
                    try {
                        // Append the seed URL:
                        final FileOutputStream fos = new FileOutputStream(
                                sourceFile, true);
                        final FileChannel chan = fos.getChannel();
                        FileLock lock = chan.lock();
                        chan.write(ByteBuffer
                                .wrap(curi.getURI().getBytes("UTF-8")));
                        lock.release();
                        chan.close();
                        fos.close();
                    } catch (IOException e) {
                        logger.log(Level.SEVERE,
                                "IOException while attempting to append to ",
                                e);
                    }
                }
            }
        }
    }

}
