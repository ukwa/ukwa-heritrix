/**
 * 
 */
package uk.bl.wap.util;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.rocksdb.BlockBasedTableConfig;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.CompactionStyle;
import org.rocksdb.CompressionType;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.springframework.context.Lifecycle;

/**
 * @author anj
 *
 */
public class RocksDBStorage implements Lifecycle {
	private RocksDB db;
	
	private String dir = "rocksDbUriUniqFilter";

	private ColumnFamilyHandle defaultCF;

	private ColumnFamilyHandle enqueuedCF;
	
    //private final int maxOpenSstFiles = 10;
	
	public RocksDBStorage( String dir ) throws IOException {
		this.dir = dir;
	}
	
    private synchronized RocksDB openDb() throws IOException {
        File path = new File(dir);

        try {
            Options options = new Options();
            options.setCreateIfMissing(true);
            configureColumnFamily(options);

            /*
             * https://github.com/facebook/rocksdb/wiki/Memory-usage-in-RocksDB "If you're
             * certain that Get() will mostly find a key you're looking for, you can set
             * options.optimize_filters_for_hits = true. With this option turned on, we will
             * not build bloom filters on the last level, which contains 90% of the
             * database. Thus, the memory usage for bloom filters will be 10X less. You will
             * pay one IO for each Get() that doesn't find data in the database, though."
             *
             * We expect a low miss rate (~17% per Alex). This setting should greatly reduce
             * memory usage.
             */
            options.setOptimizeFiltersForHits(true);

            // this one doesn't seem to be used? see dbOptions.setMaxOpenFiles()
            options.setMaxOpenFiles(256);

            DBOptions dbOptions = new DBOptions();
            dbOptions.setCreateIfMissing(true);
            dbOptions.setMaxBackgroundJobs(Math.min(8, Runtime.getRuntime().availableProcessors()));
            dbOptions.setAvoidFlushDuringRecovery(true);

            //dbOptions.setMaxOpenFiles(maxOpenSstFiles);

            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
            configureColumnFamily(cfOptions);

            List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOptions),
                    new ColumnFamilyDescriptor("enqueued-status".getBytes(UTF_8), cfOptions));

            createColumnFamiliesIfNotExists(options, dbOptions, path.toString(), cfDescriptors);
            
            List<ColumnFamilyHandle> cfHandles = new ArrayList<>(cfDescriptors.size());
            db = RocksDB.open(dbOptions, path.toString(), cfDescriptors, cfHandles);
            
            defaultCF = cfHandles.get(0);
            enqueuedCF = cfHandles.get(1);

            return db;
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }
    
    private void configureColumnFamily(Options cfOptions) throws RocksDBException {
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(22 * 1024); // approximately compresses to < 8 kB

        cfOptions.setCompactionStyle(CompactionStyle.LEVEL);
        cfOptions.setWriteBufferSize(64 * 1024 * 1024);
        cfOptions.setTargetFileSizeBase(64 * 1024 * 1024);
        cfOptions.setMaxBytesForLevelBase(512 * 1024 * 1024);
        cfOptions.setTargetFileSizeMultiplier(2);
        cfOptions.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        cfOptions.setTableFormatConfig(tableConfig);
    }

    private void configureColumnFamily(ColumnFamilyOptions cfOptions) throws RocksDBException {
        BlockBasedTableConfig tableConfig = new BlockBasedTableConfig();
        tableConfig.setBlockSize(22 * 1024); // approximately compresses to < 8 kB

        cfOptions.setCompactionStyle(CompactionStyle.LEVEL);
        cfOptions.setWriteBufferSize(64 * 1024 * 1024);
        cfOptions.setTargetFileSizeBase(64 * 1024 * 1024);
        cfOptions.setMaxBytesForLevelBase(512 * 1024 * 1024);
        cfOptions.setTargetFileSizeMultiplier(2);
        cfOptions.setCompressionType(CompressionType.SNAPPY_COMPRESSION);
        cfOptions.setTableFormatConfig(tableConfig);
    }
    
    private void createColumnFamiliesIfNotExists(Options options, DBOptions dbOptions, String path, List<ColumnFamilyDescriptor> cfDescriptors) throws RocksDBException {
        List<ColumnFamilyDescriptor> existing = new ArrayList<>();
        List<ColumnFamilyDescriptor> toCreate = new ArrayList<>();
        Set<String> cfNames = RocksDB.listColumnFamilies(options, path)
                .stream().map(bytes -> new String(bytes, UTF_8))
                .collect(Collectors.toSet());
        for (ColumnFamilyDescriptor cfDesc : cfDescriptors) {
            if (cfNames.remove(new String(cfDesc.getName(), UTF_8))) {
                existing.add(cfDesc);
            } else {
                toCreate.add(cfDesc);
            }
        }

        if (!cfNames.isEmpty()) {
            throw new RuntimeException("database may be too new: unexpected column family: " + cfNames.iterator().next());
        }

        // default CF is created automatically in empty db, exclude it
        if (existing.isEmpty()) {
            ColumnFamilyDescriptor defaultCf = cfDescriptors.get(0);
            existing.add(defaultCf);
            toCreate.remove(defaultCf);
        }

        List<ColumnFamilyHandle> handles = new ArrayList<>(existing.size());
        try (RocksDB db = RocksDB.open(dbOptions, path, existing, handles);) {
            for (ColumnFamilyDescriptor descriptor : toCreate) {
                try (ColumnFamilyHandle cf = db.createColumnFamily(descriptor)) {
                }
            }
        }
    }
    
    

	/**
	 * @return the db
	 */
	public RocksDB getDb() {
		return db;
	}

	/**
	 * @param db the db to set
	 */
	public void setDb(RocksDB db) {
		this.db = db;
	}

	/**
	 * @return the defaultCF
	 */
	public ColumnFamilyHandle getDefaultCF() {
		return defaultCF;
	}

	/**
	 * @param defaultCF the defaultCF to set
	 */
	public void setDefaultCF(ColumnFamilyHandle defaultCF) {
		this.defaultCF = defaultCF;
	}

	/**
	 * @return the enqueuedCF
	 */
	public ColumnFamilyHandle getEnqueuedCF() {
		return enqueuedCF;
	}

	/**
	 * @param enqueuedCF the enqueuedCF to set
	 */
	public void setEnqueuedCF(ColumnFamilyHandle enqueuedCF) {
		this.enqueuedCF = enqueuedCF;
	}

	@Override
	public void start() {
		try {
			this.openDb();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			throw new RuntimeException(e);
		}
	}

	@Override
	public void stop() {
		if( isRunning()) {
			try {
				this.db.syncWal();
			} catch (RocksDBException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			this.db.close();
			this.db = null;
		}
	}

	@Override
	public boolean isRunning() {
		if( this.db != null ) {
			return true;
		}
		return false;
	}

}
