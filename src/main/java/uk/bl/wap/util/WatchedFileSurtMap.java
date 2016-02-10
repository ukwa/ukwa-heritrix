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
public class WatchedFileSurtMap extends WatchedFileSource {

    private static final Logger LOGGER = Logger
            .getLogger(WatchedFileSurtMap.class.getName());

    private Map<String, Integer> currentMap = null;


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

    protected File getSourceFile() {
        if (this.recentlySeenUriUniqFilter == null
                || this.recentlySeenUriUniqFilter.getTextSource() == null)
            return null;

        return this.recentlySeenUriUniqFilter.getTextSource().getFile();
    }


    protected void loadFile() throws IOException {
        Map<String, Integer> newMap = new HashMap<String, Integer>();
        FlatFile ff = new FlatFile(this.getSourceFile().getAbsolutePath());
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
        // And assign it:
        currentMap = newMap;
        return;
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


}
