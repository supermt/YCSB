/*
 * Copyright (c) 2018 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package com.yahoo.ycsb.db.rocksdb;

import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.*;
import net.jcip.annotations.GuardedBy;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * RocksDB binding for <a href="http://rocksdb.org/">RocksDB</a>.
 * <p>
 * See {@code rocksdb/README.md} for details.
 */

// modified by Jinghuan YU
// 2019 Aug 30

public class RocksDBClient extends DB {

  static final String PROPERTY_ROCKSDB_DIR = "rocksdb.dir";
  private static final String COLUMN_FAMILY_NAMES_FILENAME = "CF_NAMES";

  private static final Logger LOGGER = LoggerFactory.getLogger(RocksDBClient.class);
  private static final java.util.logging.Logger OPLOGGER = java.util.logging.Logger.getLogger("OPs");
  private static final ConcurrentMap<String, ColumnFamily> COLUMN_FAMILIES = new ConcurrentHashMap<>();
  private static final ConcurrentMap<String, Lock> COLUMN_FAMILY_LOCKS = new ConcurrentHashMap<>();
  @GuardedBy("RocksDBClient.class")
  private static Path rocksDbDir = null;
  @GuardedBy("RocksDBClient.class")
  private static RocksObject dbOptions = null;
  @GuardedBy("RocksDBClient.class")
  private static RocksDB rocksDb = null;
  @GuardedBy("RocksDBClient.class")
  private static int references = 0;

  @Override
  public void init() throws DBException {
    synchronized (RocksDBClient.class) {
      if (rocksDb == null) {
        rocksDbDir = Paths.get(getProperties().getProperty(PROPERTY_ROCKSDB_DIR));
        LOGGER.info("RocksDB data dir: " + rocksDbDir);

        try {
          rocksDb = initRocksDB();
        } catch (final IOException | RocksDBException e) {
          throw new DBException(e);
        }
      }

      references++;
    }
  }

  /**
   * Initializes and opens the RocksDB database.
   * <p>
   * Should only be called with a {@code synchronized(RocksDBClient.class)` block}.
   *
   * @return The initialized and open RocksDB instance.
   */
  private RocksDB initRocksDB() throws IOException, RocksDBException {
    LOGGER.info("Initializing the RocksDB, checking the target directory");

    if (!Files.exists(rocksDbDir)) {
      Files.createDirectories(rocksDbDir);
    }

    // use an additional file logger to record the operations;
    Properties properties = this.getProperties();
    String op = properties.getProperty(com.yahoo.ycsb.Client.DO_TRANSACTIONS_PROPERTY);
    // use this sentence to remove console prints.
    OPLOGGER.setUseParentHandlers(false);

    if (Boolean.parseBoolean(op)) {
      op = "/run_log.txt";
    } else {
      op = "/load_log.txt";
    }

    FileHandler file = new FileHandler(rocksDbDir.toAbsolutePath().toString() + op);
    OPLOGGER.addHandler(file);
    file.setFormatter(new TimelessFormat());


    final List<String> cfNames = loadColumnFamilyNames();
    final List<ColumnFamilyOptions> cfOptionss = new ArrayList<>();
    final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();

    for (final String cfName : cfNames) {
      final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions()
          .optimizeLevelStyleCompaction();
      final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(
          cfName.getBytes(UTF_8),
          cfOptions
      );
      cfOptionss.add(cfOptions);
      cfDescriptors.add(cfDescriptor);
    }

    LOGGER.info("Allocating the thread resources");

    final int rocksThreads = Runtime.getRuntime().availableProcessors() * 2;

    if (cfDescriptors.isEmpty()) {
      final Options options = new Options()
          .optimizeLevelStyleCompaction()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;
      return RocksDB.open(options, rocksDbDir.toAbsolutePath().toString());
    } else {
      final DBOptions options = new DBOptions()
          .setCreateIfMissing(true)
          .setCreateMissingColumnFamilies(true)
          .setIncreaseParallelism(rocksThreads)
          .setMaxBackgroundCompactions(rocksThreads)
          .setInfoLogLevel(InfoLogLevel.INFO_LEVEL);
      dbOptions = options;

      final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
      final RocksDB db = RocksDB.open(options, rocksDbDir.toAbsolutePath().toString(), cfDescriptors, cfHandles);
      for (int i = 0; i < cfNames.size(); i++) {
        COLUMN_FAMILIES.put(cfNames.get(i), new ColumnFamily(cfHandles.get(i), cfOptionss.get(i)));
      }

      LOGGER.info("DB created");

      return db;
    }
  }

  @Override
  public void cleanup() throws DBException {
    super.cleanup();

    synchronized (RocksDBClient.class) {
      try {
        if (references == 1) {
          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getHandle().close();
          }

          rocksDb.close();
          rocksDb = null;

          dbOptions.close();
          dbOptions = null;

          for (final ColumnFamily cf : COLUMN_FAMILIES.values()) {
            cf.getOptions().close();
          }
          saveColumnFamilyNames();
          COLUMN_FAMILIES.clear();

          rocksDbDir = null;
        }

      } catch (final IOException e) {
        throw new DBException(e);
      } finally {
        references--;
      }
    }
  }

  // read is looking up, for a certain result
  @Override
  public Status read(final String table, final String key, final Set<String> fields,
                     final Map<String, ByteIterator> result) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      final byte[] values = rocksDb.get(cf, key.getBytes(UTF_8));
      if (values == null) {
        return Status.NOT_FOUND;
      }
      deserializeValues(values, fields, result);
      OPLOGGER.info("Point," + key);
      return Status.OK;
    } catch (final RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  // Scan is the range query.
  @Override
  public Status scan(final String table, final String startkey, final int recordcount, final Set<String> fields,
                     final Vector<HashMap<String, ByteIterator>> result) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      try (final RocksIterator iterator = rocksDb.newIterator(cf)) {
        int iterations = 0;
        for (iterator.seek(startkey.getBytes(UTF_8)); iterator.isValid() && iterations < recordcount;
             iterator.next()) {
          final HashMap<String, ByteIterator> values = new HashMap<>();
          deserializeValues(iterator.value(), fields, values);
          result.add(values);
          iterations++;
        }
      }

      OPLOGGER.info("Scan," + startkey + "@" + recordcount);
      return Status.OK;
    } catch (final RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  // return none if not exist, it won't create a new one.
  // it's not practice in real world data, Rocksdb is a LSM
  // which means it won't look up the key before update.
  @Override
  public Status update(final String table, final String key, final Map<String, ByteIterator> values) {
    //TODO(AR) consider if this would be faster with merge operator

    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      final Map<String, ByteIterator> result = new HashMap<>();
// modified by Jinghuan YU
//      final byte[] currentValues = rocksDb.get(cf, key.getBytes(UTF_8));
//      if(currentValues == null) {
//        return Status.NOT_FOUND;
//      }
//      deserializeValues(currentValues, null, result);

      //update
      result.putAll(values);

      //store
      rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(result));

      OPLOGGER.info("update," + key);
      return Status.OK;

    } catch (final RocksDBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(final String table, final String key, final Map<String, ByteIterator> values) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      rocksDb.put(cf, key.getBytes(UTF_8), serializeValues(values));
      OPLOGGER.info("insert," + key);
      return Status.OK;
    } catch (final RocksDBException | IOException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(final String table, final String key) {
    try {
      if (!COLUMN_FAMILIES.containsKey(table)) {
        createColumnFamily(table);
      }

      final ColumnFamilyHandle cf = COLUMN_FAMILIES.get(table).getHandle();
      rocksDb.delete(cf, key.getBytes(UTF_8));
      OPLOGGER.info("delete," + key);
      return Status.OK;
    } catch (final RocksDBException e) {
      LOGGER.error(e.getMessage(), e);
      return Status.ERROR;
    }
  }

  private void saveColumnFamilyNames() throws IOException {
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    try (final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(file, UTF_8))) {
      writer.println(new String(RocksDB.DEFAULT_COLUMN_FAMILY, UTF_8));
      for (final String cfName : COLUMN_FAMILIES.keySet()) {
        writer.println(cfName);
      }
    }
  }

  private List<String> loadColumnFamilyNames() throws IOException {
    final List<String> cfNames = new ArrayList<>();
    final Path file = rocksDbDir.resolve(COLUMN_FAMILY_NAMES_FILENAME);
    if (Files.exists(file)) {
      try (final LineNumberReader reader =
               new LineNumberReader(Files.newBufferedReader(file, UTF_8))) {
        String line = null;
        while ((line = reader.readLine()) != null) {
          cfNames.add(line);
        }
      }
    }
    return cfNames;
  }

  private Map<String, ByteIterator> deserializeValues(final byte[] values, final Set<String> fields,
                                                      final Map<String, ByteIterator> result) {
    final ByteBuffer buf = ByteBuffer.allocate(4);

    int offset = 0;
    while (offset < values.length) {
      buf.put(values, offset, 4);
      buf.flip();
      final int keyLen = buf.getInt();
      buf.clear();
      offset += 4;

      final String key = new String(values, offset, keyLen);
      offset += keyLen;

      buf.put(values, offset, 4);
      buf.flip();
      final int valueLen = buf.getInt();
      buf.clear();
      offset += 4;

      if (fields == null || fields.contains(key)) {
        result.put(key, new ByteArrayByteIterator(values, offset, valueLen));
      }

      offset += valueLen;
    }

    return result;
  }

  private byte[] serializeValues(final Map<String, ByteIterator> values) throws IOException {
    try (final ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
      final ByteBuffer buf = ByteBuffer.allocate(4);

      for (final Map.Entry<String, ByteIterator> value : values.entrySet()) {
        final byte[] keyBytes = value.getKey().getBytes(UTF_8);
        final byte[] valueBytes = value.getValue().toArray();

        buf.putInt(keyBytes.length);
        baos.write(buf.array());
        baos.write(keyBytes);

        buf.clear();

        buf.putInt(valueBytes.length);
        baos.write(buf.array());
        baos.write(valueBytes);

        buf.clear();
      }
      return baos.toByteArray();
    }
  }

  private void createColumnFamily(final String name) throws RocksDBException {
    COLUMN_FAMILY_LOCKS.putIfAbsent(name, new ReentrantLock());

    final Lock l = COLUMN_FAMILY_LOCKS.get(name);
    l.lock();
    try {
      if (!COLUMN_FAMILIES.containsKey(name)) {
        final ColumnFamilyOptions cfOptions = new ColumnFamilyOptions().optimizeLevelStyleCompaction();
        final ColumnFamilyHandle cfHandle = rocksDb.createColumnFamily(
            new ColumnFamilyDescriptor(name.getBytes(UTF_8), cfOptions)
        );
        COLUMN_FAMILIES.put(name, new ColumnFamily(cfHandle, cfOptions));
      }
    } finally {
      l.unlock();
    }
  }

  private static final class ColumnFamily {
    private final ColumnFamilyHandle handle;
    private final ColumnFamilyOptions options;

    private ColumnFamily(final ColumnFamilyHandle handle, final ColumnFamilyOptions options) {
      this.handle = handle;
      this.options = options;
    }

    public ColumnFamilyHandle getHandle() {
      return handle;
    }

    public ColumnFamilyOptions getOptions() {
      return options;
    }
  }

  private class TimelessFormat extends Formatter {
    @Override
    public String format(LogRecord logRecord) {
      return logRecord.getMessage() + "\n";
    }
  }

  private class TimingFormat extends Formatter {
    @Override
    public String format(LogRecord logRecord) {
      return logRecord.getMillis() + logRecord.getMessage() + "\n";
    }
  }
}
