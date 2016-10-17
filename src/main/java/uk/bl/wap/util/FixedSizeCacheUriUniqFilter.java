/**
 * 
 */
package uk.bl.wap.util;

import org.apache.commons.codec.binary.StringUtils;
import org.archive.crawler.util.SetBasedUriUniqFilter;

/**
 * 
 * This cache-based unique URI filter limits memory usage while ensuring no
 * false-positives, i.e. it never says it has seen an URI when it has not.
 * Instead, it will sometimes emit false-negatives instead.
 * 
 * Note that because this needs we must store the keys in an array, the memory
 * usage is much higher than for a Bloom filter.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class FixedSizeCacheUriUniqFilter extends SetBasedUriUniqFilter {

    /**
     * org.archive.util.CLibrary
     */
    private static final long serialVersionUID = 3412882684216842032L;

    private ByteArrayFilter cache = new ByteArrayFilter(100 * 1000);
    private long total = 0;

    /* (non-Javadoc)
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setAdd(java.lang.CharSequence)
     */
    @Override
    protected boolean setAdd(CharSequence key) {
        boolean contained = cache
                .containsAndAdd(StringUtils.getBytesUtf8(key.toString()));
        if (!contained) {
            total++;
        }
        return !contained;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setRemove(java.lang.CharSequence)
     */
    @Override
    protected boolean setRemove(CharSequence key) {
        new Exception().printStackTrace();
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setCount()
     */
    @Override
    protected long setCount() {
        return total;
    }

}
