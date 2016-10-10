/**
 * 
 */
package uk.bl.wap.util;

import org.apache.commons.codec.binary.StringUtils;
import org.archive.crawler.util.SetBasedUriUniqFilter;

/**
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class FixedSizeCacheUriUniqFilter extends SetBasedUriUniqFilter {

    /**
     * 
     */
    private static final long serialVersionUID = 3412882684216842032L;

    private ByteArrayFilter cache = new ByteArrayFilter(1000000000);
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
