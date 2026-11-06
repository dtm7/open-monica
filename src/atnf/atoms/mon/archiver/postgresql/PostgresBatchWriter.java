package atnf.atoms.mon.archiver.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import org.apache.log4j.Logger;

import atnf.atoms.mon.archiver.postgresql.PostgresBatchPoint;

public class PostgresBatchWriter {
  /** Logger. */
  protected Logger itsLogger = Logger.getLogger(getClass().getName());

  private final ConcurrentLinkedDeque<PostgresBatchPoint> buffer = new ConcurrentLinkedDeque<>();

  // Try with modest thread count values to begin with
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final ExecutorService writerPool = Executors.newFixedThreadPool(2);

  private final javax.sql.DataSource dataSource;
  private final int maxBatchAge;
  private final int maxBatchSize;
  private final int maxBufferSize;
  private final long shutdownTimeout;

  // Pause between drain attempts at shutdown when the previous attempt made no progress
  private static final long SHUTDOWN_RETRY_PAUSE_MS = 1000;

  /**
   * @param maxBatchAge     Maximum time (ms) between scheduled flushes
   * @param maxBatchSize    Maximum number of points written per batch
   * @param maxBufferSize   Maximum number of points held in memory; oldest are dropped beyond this
   * @param shutdownTimeout Maximum time (ms) to spend draining the buffer at shutdown
   */
  public PostgresBatchWriter(javax.sql.DataSource dataSource, int maxBatchAge, int maxBatchSize, int maxBufferSize,
                             long shutdownTimeout) {
    this.dataSource = dataSource;
    this.maxBatchAge = maxBatchAge;
    this.maxBatchSize = maxBatchSize;
    this.maxBufferSize = maxBufferSize;
    this.shutdownTimeout = shutdownTimeout;
  }

  public void start() {
    // Start background flusher task, flush data every ${maxBatchAge} ms
    scheduler.scheduleAtFixedRate(this::flushDB, maxBatchAge, maxBatchAge, TimeUnit.MILLISECONDS);
  }

  // Main public interface with the posgres batch writer. Add a data point to
  // the queue, and if the queue is larger than ${N} entries, trigger a manual
  // flush
  
  public void enqueue(PostgresBatchPoint data) {
    // Buffer too large, remove oldest value (front of queue)
    if (buffer.size() >= maxBufferSize) buffer.pollFirst();

    // Add the new point to the end of the buffer
    buffer.addLast(data);

    // trigger early flush if threshold reached. Once shutdown() has been called the pool
    // rejects new tasks; the shutdown drain will pick up anything still in the buffer.
    if (buffer.size() >= maxBatchSize && !writerPool.isShutdown()) {
      try {
        writerPool.submit(this::flushDB);
      } catch (RejectedExecutionException e) {
        // Lost the race with shutdown(); nothing to do
      }
    }
  }

  // Do an orderly shutdown of the postgres batch writer. First stop the background
  // flush threads, then drain the buffer synchronously until it is empty or
  // ${shutdownTimeout} ms have passed.

  public void shutdown() {
    itsLogger.info("shutdown: Shutting down postgres writer threads");
    scheduler.shutdown();
    writerPool.shutdown();
    try {
      // Wait for the batch delay time for any in-progress flushes to finish
      if (!scheduler.awaitTermination(maxBatchAge, TimeUnit.MILLISECONDS)) {
        scheduler.shutdownNow();
      }
      if (!writerPool.awaitTermination(maxBatchAge, TimeUnit.MILLISECONDS)) {
        writerPool.shutdownNow();
      }
    } catch (InterruptedException e) {
      scheduler.shutdownNow();
      writerPool.shutdownNow();
      Thread.currentThread().interrupt();
    }

    drainBuffer();
  }

  // Flush the buffer to the database until it is empty or the shutdown timeout expires.
  // Progress is measured by the buffer shrinking: a batch that fails with a retryable
  // error is put back on the queue, so it does not count as progress. Note the deadline
  // is checked between attempts, so a slow connection attempt (driver connect timeout)
  // can overrun it somewhat.

  private void drainBuffer() {
    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(shutdownTimeout);
    int remaining = buffer.size();
    int initial = remaining;

    if (remaining > 0) {
      itsLogger.info("shutdown: Flushing " + remaining + " buffered points to database (timeout " +
                     (shutdownTimeout / 1000) + " s)");
    }

    while (remaining > 0 && System.nanoTime() < deadline) {
      flushDB();
      int now = buffer.size();
      if (now >= remaining) {
        // No progress (database unreachable, or batch re-queued); wait before retrying
        try {
          Thread.sleep(SHUTDOWN_RETRY_PAUSE_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
      remaining = now;
    }

    if (remaining > 0) {
      itsLogger.error("shutdown: Could not write " + remaining + " of " + initial +
                      " buffered points to database before shutdown; these points are lost");
    } else if (initial > 0) {
      itsLogger.info("shutdown: All " + initial + " buffered points written to database");
    }
  }

  // Every insert statement should be the same format so the SQL driver can batch all the inserts
  // together for performance. We add "on conflict do nothing" so that tripping potential
  // unique constraints (ts, point_id, source) does not lose the whole batch.
  private static final String INSERT_SQL = "INSERT INTO archive " +
              "(ts, bat, point_id, source, val_float, val_bigint, val_int, val_string, val_bool) " +
              "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
              "ON CONFLICT DO NOTHING";

  // Maximum number of individually rejected rows to log per batch, to avoid log floods
  private static final int MAX_ROW_ERRORS_LOGGED = 10;

  // PostgreSQL text values cannot contain NUL (0x00) characters; inserting one fails with
  // SQLState 22021. Replace them with U+FFFD (the Unicode replacement character) so the
  // value is still stored, with a visible marker that something was substituted.
  static String sanitiseText(String s) {
    if (s == null || s.indexOf('\u0000') < 0) return s;
    return s.replace('\u0000', '\uFFFD');
  }

  // Bind a single data point to the parameters of INSERT_SQL
  private static void bindRow(PreparedStatement ps, PostgresBatchPoint data) throws SQLException {
    ps.setObject(1, data.timestamp);
    ps.setObject(2, data.bat);
    ps.setObject(3, data.pointid);
    ps.setString(4, sanitiseText(data.source));

    // Which data column gets a value depends on the data type
    // of this data point

    if (data.value instanceof Double) {
      ps.setDouble(5, (Double) data.value);
    } else {
      ps.setNull(5, java.sql.Types.FLOAT);
    }

    if (data.value instanceof Long) {
      ps.setLong(6, (Long) data.value);
    } else {
      ps.setNull(6, java.sql.Types.BIGINT);
    }

    if (data.value instanceof Integer) {
      ps.setInt(7, (Integer) data.value);
    } else {
      ps.setNull(7, java.sql.Types.INTEGER);
    }

    if (data.value instanceof String) {
      ps.setString(8, sanitiseText((String) data.value));
    } else {
      ps.setNull(8, java.sql.Types.VARCHAR);
    }

    if (data.value instanceof Boolean) {
      ps.setBoolean(9, (Boolean) data.value);
    } else {
      ps.setNull(9, java.sql.Types.BOOLEAN);
    }
  }

  // Put points back at the front of the queue, preserving their order
  private void requeue(List<PostgresBatchPoint> points) {
    for (int i = points.size() - 1; i >= 0; i--) {
      buffer.addFirst(points.get(i));
    }
  }

  // Write a batch of data to the postgres database

  private void writeBatch(Connection conn, List<PostgresBatchPoint> batch) {
    // Construct the prepared statement and try and write it
    try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
      // Allow the SQL driver to auto batch our inserts
      conn.setAutoCommit(false);
      for (PostgresBatchPoint data : batch) {
        bindRow(ps, data);
        ps.addBatch();
      }

      ps.executeBatch();
      itsLogger.debug("insertData: Comitting " + batch.size() + " data points to database");
      conn.commit();
    } catch (SQLException e) {
      // Leave the connection clean before it goes back to the pool
      try {
        conn.rollback();
      } catch (SQLException re) {
        itsLogger.debug("insertData: Rollback after failed batch also failed: " + re.getMessage());
      }

      String state = findSQLState(e);
      if (isRetryable(e)) {
        itsLogger.error("insertData: Retryable SQL error (SQLState " + state + "), re-adding " + batch.size() +
                        " points to queue: " + e.getMessage());
        requeue(batch);
      } else if (batch.size() > 1 && isRowLevel(e)) {
        // One (or a few) bad rows caused the whole batch to be rolled back. Write the rows
        // individually so that only the offending ones are lost.
        itsLogger.warn("insertData: Batch of " + batch.size() + " rejected (SQLState " + state +
                       "), retrying row by row: " + e.getMessage());
        writeRowByRow(conn, batch);
      } else {
        itsLogger.error("insertData: Discarding " + batch.size() + " points, non-retryable SQL error (SQLState " +
                        state + "): " + e.getMessage());
      }
    }
  }

  // Fallback path: insert each row in its own transaction, dropping (and logging) only the
  // rows the database rejects. If the database becomes unavailable part way through, the
  // current row and everything after it is put back on the queue.

  private void writeRowByRow(Connection conn, List<PostgresBatchPoint> batch) {
    int written = 0;
    int dropped = 0;

    try {
      conn.setAutoCommit(true);
    } catch (SQLException e) {
      itsLogger.error("insertData: Could not enable autocommit for row-by-row retry, re-adding " +
                      batch.size() + " points to queue: " + e.getMessage());
      requeue(batch);
      return;
    }

    try (PreparedStatement ps = conn.prepareStatement(INSERT_SQL)) {
      for (int i = 0; i < batch.size(); i++) {
        PostgresBatchPoint data = batch.get(i);
        try {
          bindRow(ps, data);
          ps.executeUpdate();
          written++;
        } catch (SQLException e) {
          if (isRetryable(e)) {
            List<PostgresBatchPoint> rest = batch.subList(i, batch.size());
            itsLogger.error("insertData: Retryable SQL error during row-by-row retry (SQLState " + findSQLState(e) +
                            "), re-adding " + rest.size() + " points to queue: " + e.getMessage());
            requeue(rest);
            break;
          }
          dropped++;
          if (dropped <= MAX_ROW_ERRORS_LOGGED) {
            itsLogger.error("insertData: Dropping point_id=" + data.pointid + " source=" + data.source +
                            " ts=" + data.timestamp + " (SQLState " + findSQLState(e) + "): " + e.getMessage());
          }
        }
      }
    } catch (SQLException e) {
      // Failed to even prepare the statement; nothing has been written yet
      itsLogger.error("insertData: Could not prepare row-by-row insert, re-adding " + batch.size() +
                      " points to queue: " + e.getMessage());
      requeue(batch);
      return;
    }

    if (dropped > MAX_ROW_ERRORS_LOGGED) {
      itsLogger.error("insertData: ... " + (dropped - MAX_ROW_ERRORS_LOGGED) + " further rejected rows not logged");
    }
    itsLogger.info("insertData: Row-by-row retry wrote " + written + ", dropped " + dropped + " of " + batch.size() + " points");
  }

  // Errors caused by the content of individual rows, where retrying the rows one at a time
  // will salvage the good ones: 22xxx data exceptions (value too long, out of range, invalid
  // text) and 23xxx integrity constraint violations (not null, foreign key, check).
  // Errors like 42P01 (missing table) affect every row, so row-by-row retry would be pointless.

  static boolean isRowLevel(SQLException e) {
    String s = findSQLState(e);
    return s != null && (s.startsWith("22") || s.startsWith("23"));
  }

  // Decide whether a failed batch is worth retrying. The PostgreSQL JDBC driver throws
  // PSQLException, which extends SQLException directly rather than the JDBC 4
  // SQLTransientException/SQLRecoverableException subclasses, so we classify by SQLState.
  //
  //   08xxx  connection exception (server gone, network drop, connection closed)
  //   40xxx  transaction rollback (serialization failure, deadlock)
  //   53xxx  insufficient resources (disk full, out of memory, too many connections)
  //   57P01  admin_shutdown, 57P02 crash_shutdown, 57P03 cannot_connect_now
  //
  // Anything else (data errors, constraint violations, missing tables...) would fail again
  // identically, so retrying it would just wedge the head of the queue.

  static boolean isRetryable(SQLException e) {
    for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
      if (cur instanceof SQLTransientException || cur instanceof SQLRecoverableException) {
        return true;
      }
      String s = cur.getSQLState();
      if (s != null && (s.startsWith("08") || s.startsWith("40") || s.startsWith("53") ||
                        s.equals("57P01") || s.equals("57P02") || s.equals("57P03"))) {
        return true;
      }
      // Guard against a chain that loops back on itself
      if (cur.getNextException() == cur) break;
    }
    return false;
  }

  // BatchUpdateException often carries the useful SQLState on a chained exception
  private static String findSQLState(SQLException e) {
    for (SQLException cur = e; cur != null; cur = cur.getNextException()) {
      if (cur.getSQLState() != null) return cur.getSQLState();
      if (cur.getNextException() == cur) break;
    }
    return null;
  }

  // Simple function which is called by our multiple threads, will prepare and write
  // a batch of up to ${N} points into the database

  private void flushDB() {
    List<PostgresBatchPoint> batch = new ArrayList<>(maxBatchSize);

    try (Connection conn = dataSource.getConnection()) {
      // Explicitly check whether the connection we have is valid. Before we do any
      // manipulation of the buffer.

      if (conn == null || !conn.isValid(5)) {
        itsLogger.error("flushDB: Database connection invalid");
        return;
      }

      while (batch.size() < maxBatchSize && !buffer.isEmpty()) {
        PostgresBatchPoint item = buffer.pollFirst();
        if (item != null) batch.add(item);
      }

      if (!batch.isEmpty()) {
        writeBatch(conn, batch);
      }

    } catch (SQLException e) {
      // If get an error getting a new database connection, return early without touching
      // the buffer at all. This should hopefully make us fault tolerant to intermittent
      // database communication issues.

      itsLogger.error("flushDB: Error creating database connection: " + e.getMessage());
    }
  }
}
