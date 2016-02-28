/**
 * 
 */
package uk.bl.wap.crawler.modules.seeds;

import java.io.File;
import java.util.logging.Logger;

import org.archive.modules.CrawlURI;
import org.archive.modules.seeds.SeedModule;

/**
 * 
 * A dummy seeds module for use when other mechanisms are being used to inject
 * and manage seeds.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class NoOpSeedsModule extends SeedModule {
    private static final Logger LOGGER = Logger
            .getLogger(NoOpSeedsModule.class.getName());

    /** */
    private static final long serialVersionUID = -406997411357519080L;

    /* (non-Javadoc)
     * @see org.archive.modules.seeds.SeedModule#announceSeeds()
     */
    @Override
    public void announceSeeds() {
        LOGGER.fine("Ignoring accounceSeeds()");
    }

    /* (non-Javadoc)
     * @see org.archive.modules.seeds.SeedModule#actOn(java.io.File)
     */
    @Override
    public void actOn(File f) {
        LOGGER.fine("Ignoring actOn(File) " + f.getAbsolutePath());
    }

    /* (non-Javadoc)
     * @see org.archive.modules.seeds.SeedModule#addSeed(org.archive.modules.CrawlURI)
     */
    @Override
    public void addSeed(CrawlURI curi) {
        LOGGER.fine("Ignoring addSeed(CrawlURI) " + curi.getURI());
    }

}
