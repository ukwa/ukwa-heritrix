/**
 * 
 */
package uk.bl.wap.modules.uriuniqfilters;

import org.apache.hadoop.hbase.util.Hash;
import org.apache.hadoop.util.bloom.CountingBloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.archive.crawler.util.SetBasedUriUniqFilter;

/**
 * 
 * An experiment in using a CountingBloomFilter
 * 
 * Note really much use unless it gets saved/loaded on checkpoints.
 * 
 * See BdbUriUniqFilter etc.
 * 
 * Also need to make the size tunable in a sensible fashion.
 * 
 * @author Andrew Jackson <Andrew.Jackson@bl.uk>
 *
 */
public class CountingBloomUriUniqFilter extends SetBasedUriUniqFilter {

    /**
     * 
     */
    private static final long serialVersionUID = 3678493572967397929L;

    /* The CountingBloomFilter */
    CountingBloomFilter cbf = new CountingBloomFilter(100000, 4,
            Hash.MURMUR_HASH3);

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setAdd(java.lang.
     * CharSequence)
     * 
     * @return true if the item is new and is being added to the set of known
     * URIs
     */
    @Override
    protected boolean setAdd(CharSequence key) {
        Key bkey = new Key(key.toString().getBytes());
        boolean isMember = cbf.membershipTest(bkey);
        if (isMember) {
            return false;
        } else {
            cbf.add(bkey);
            return true;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setRemove(java.lang.
     * CharSequence)
     * 
     * @return true if the item was in the set of known URIs
     */
    @Override
    protected boolean setRemove(CharSequence key) {
        Key bkey = new Key(key.toString().getBytes());
        boolean isMember = cbf.membershipTest(bkey);
        if (isMember) {
            cbf.delete(bkey);
            return true;
        }
        return false;
    }

    /* (non-Javadoc)
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setCount()
     */
    @Override
    protected long setCount() {
        return 0;
    }

}
