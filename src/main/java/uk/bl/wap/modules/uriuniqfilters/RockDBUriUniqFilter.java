/**
 * 
 */
package uk.bl.wap.modules.uriuniqfilters;

import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;
import org.archive.crawler.util.SetBasedUriUniqFilter;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import uk.bl.wap.util.RocksDBStorage;

/**
 * @author anj
 *
 */
public class RockDBUriUniqFilter extends SetBasedUriUniqFilter {

    /**
	 * 
	 */
	private static final long serialVersionUID = -7106621944096696557L;
	
	private static Logger LOGGER =
	        Logger.getLogger(RockDBUriUniqFilter.class.getName());
	

	private AtomicLong setCount = new AtomicLong(0); 
    
    private byte[] flagb = new byte[]{1};

    private RocksDBStorage rocksDbStorage;
    
    /**
	 * @return the rocksDbStorage
	 */
	public RocksDBStorage getRocksDbStorage() {
		return rocksDbStorage;
	}

	/**
	 * @param rocksDbStorage the rocksDbStorage to set
	 */
	public void setRocksDbStorage(RocksDBStorage rocksDbStorage) {
		this.rocksDbStorage = rocksDbStorage;
	}

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
		RocksDB db = this.rocksDbStorage.getDb();
		ColumnFamilyHandle enqueuedCF = this.rocksDbStorage.getEnqueuedCF();
		//
		byte[] keyb = key.toString().getBytes();
		try {
			byte[] entry = db.get(enqueuedCF, keyb);
			if (entry == null) {
				// New!
				db.put(enqueuedCF, keyb, flagb);
				this.addedCount.incrementAndGet();
				LOGGER.fine("Added " + key);
				return true;
			} else {
				return false;
			}
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
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
		RocksDB db = this.rocksDbStorage.getDb();
		ColumnFamilyHandle enqueuedCF = this.rocksDbStorage.getEnqueuedCF();
		//
		byte[] keyb = key.toString().getBytes();
		try {
			byte[] entry = db.get(enqueuedCF, keyb);
			if (entry != null) {
				// Known!
				db.delete(enqueuedCF, keyb);
				this.setCount.decrementAndGet();
				LOGGER.fine("Removed " + key);
				return true;
			} else {
				return false;
			}
		} catch (RocksDBException e) {
			throw new RuntimeException(e);
		}
	}

    /* (non-Javadoc)
     * @see org.archive.crawler.util.SetBasedUriUniqFilter#setCount()
     */
	@Override
	protected long setCount() {
		// TODO Auto-generated method stub
		return setCount.get();
	}

}
