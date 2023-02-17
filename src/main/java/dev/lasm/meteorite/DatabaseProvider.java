package dev.lasm.meteorite;

import org.rocksdb.RocksDB;

public interface DatabaseProvider {
    RocksDB getDb();

    String getDatabaseId();

    /**
     * Close the database after all transactions.
     */
    void queueClose();
}
