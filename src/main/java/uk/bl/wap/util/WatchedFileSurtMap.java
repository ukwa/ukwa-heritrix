/**
 * 
 */
package uk.bl.wap.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import org.archive.surt.SURTTokenizer;

import uk.bl.wap.modules.uriuniqfilters.RecentlySeenUriUniqFilter;

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


    private RecentlySeenUriUniqFilter recentlySeenUriUniqFilter;

    public WatchedFileSurtMap(
            RecentlySeenUriUniqFilter recentlySeenUriUniqFilter) {
        this.recentlySeenUriUniqFilter = recentlySeenUriUniqFilter;
    }

    protected File getSourceFile() {
        if (this.recentlySeenUriUniqFilter == null
                || this.recentlySeenUriUniqFilter.getTextSource() == null)
            return null;

        return this.recentlySeenUriUniqFilter.getTextSource().getFile();
    }


    protected void loadFile() throws IOException {
        Map<String, Integer> newMap = new HashMap<String, Integer>();
        File ff = new File(this.getSourceFile().getAbsolutePath());
        FileInputStream itr = new FileInputStream(ff);
        BufferedReader br = new BufferedReader(new InputStreamReader(itr));
        LOGGER.fine("EXCLUSION-MAP: looking at " + ff.getAbsolutePath());
        String line;
        while ((line = br.readLine()) != null) {
            line = line.trim();
            LOGGER.fine("EXCLUSION-MAP: looking at " + line);

            if (line.length() == 0) {
                continue;
            }

            String[] parts = line.split(" ", 2);
            String key = parts[0];
            String surt = key.startsWith("(") ? key
                    : SURTTokenizer.prefixKey(key);

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
