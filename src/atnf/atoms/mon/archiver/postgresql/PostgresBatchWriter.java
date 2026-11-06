package atnf.atoms.mon.archiver.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.sql.SQLRecoverableException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.log4j.Logger;

import atnf.atoms.mon.archiver.postgresql.PostgresBatchPoint;

public class PostgresBatchWriter {
  /** Logger. */
  protected Logger itsLogger = Logger.getLogger(getClass().getName());

  private final ConcurrentLinkedDeque<PostgresBatchPoint> buffer = new ConcurrentLinkedDeque<>();

  // ConcurrentLinkedDeque.size() walks the whole list, so track the size separately.
  // Incremented after every add and decremented after every successful poll; it may be
  // off by a point or two transiently under concurrency, which is fine for flush decisions.
  private final AtomicInteger bufferCount = new AtomicInteger();

  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final ExecutorService writerPool;

  private final javax.sql.DataSource dataSource;
  private final int maxBatchAge;
  private final int maxBatchSize;
  private final int maxBufferSize;
  private final long shutdownTimeout;
  private final int writerThreads;

  // Number of triggered flush tasks currently queued or running (at most writerThreads)
  private final AtomicInteger pendingFlushes = new AtomicInteger();

  // After a triggered flush fails to make progress, enqueue() does not trigger another one
  // until this System.nanoTime() value; the scheduled flush keeps retrying in the meantime
  private volatile long triggerBackoffUntil = System.nanoTime();

  // Points dropped because the buffer was full, not yet reported in the log
  private final AtomicLong droppedPoints = new AtomicLong();
  private final AtomicLong lastDropWarning = new AtomicLong(System.nanoTime() - DROP_WARNING_INTERVAL_NS);

  // Pause between drain attempts at shutdown when the previous attempt made no progress
  private static final long SHUTDOWN_RETRY_PAUSE_MS = 1000;

  // Minimum interval between "buffer full" warnings
  private static final long DROP_WARNING_INTERVAL_NS = TimeUnit.SECONDS.toNanos(60);

  /**
   * @param maxBatchAge     Maximum time (ms) between scheduled flushes. Also used as the
   *                        back-off before triggering another flush after a failed one.
   * @param maxBatchSize    Maximum number of points written per batch
   * @param maxBufferSize   Maximum number of points held in memory; oldest are dropped beyond this
   * @param shutdownTimeout Maximum time (ms) to spend draining the buffer at shutdown
   * @param writerThreads   Number of threads writing full batches in parallel (minimum 1)
   */
  public PostgresBatchWriter(javax.sql.DataSource dataSource, int maxBatchAge, int maxBatchSize, int maxBufferSize,
                             long shutdownTimeout, int writerThreads) {
    this.dataSource = dataSource;
    this.maxBatchAge = maxBatchAge;
    this.maxBatchSize = maxBatchSize;
    this.maxBufferSize = maxBufferSize;
    this.shutdownTimeout = shutdownTimeout;
    this.writerThreads = Math.max(1, writerThreads);
    this.writerPool = Executors.newFixedThreadPool(this.writerThreads);
  }

  public void start() {
    // Start background flusher task, flush data every ${maxBatchAge} ms
    scheduler.scheduleAtFixedRate(this::flushDB, maxBatchAge, maxBatchAge, TimeUnit.MILLISECONDS);
  }

  // Main public interface with the posgres batch writer. Add a data point to
  // the queue, and if at least one full batch is buffered, trigger a flush

  public void enqueue(PostgresBatchPoint data) {
    // Buffer too large, remove oldest value (front of queue)
    if (bufferCount.get() >= maxBufferSize && buffer.pollFirst() != null) {
      bufferCount.decrementAndGet();
      droppedPoints.incrementAndGet();
      warnDroppedPoints(false);
    }

    // Add the new point to the end of the buffer
    buffer.addLast(data);
    bufferCount.incrementAndGet();

    maybeTriggerFlush();
  }

  // Submit a flush task if a full batch is waiting, fewer than ${writerThreads} flushes are
  // already pending, and we are not backing off after a failure. Once shutdown() has been
  // called the pool rejects new tasks; the shutdown drain picks up anything still buffered.

  private void maybeTriggerFlush() {
    if (bufferCount.get() < maxBatchSize || writerPool.isShutdown()) return;
    if (System.nanoTime() - triggerBackoffUntil < 0) return;

    int pending;
    do {
      pending = pendingFlushes.get();
      if (pending >= writerThreads) return;
    } while (!pendingFlushes.compareAndSet(pending, pending + 1));

    try {
      writerPool.submit(this::triggeredFlush);
    } catch (RejectedExecutionException e) {
      // Lost the race with shutdown(); nothing to do
      pendingFlushes.decrementAndGet();
    }
  }

  // Body of a triggered flush: keep writing batches while at least one full batch is
  // buffered, so a backlog drains in one task rather than one task per enqueued point.
  // Stops when the pool is shut down (the shutdown drain takes over) or when a flush makes
  // no progress, in which case further triggers are suppressed for ${maxBatchAge} ms.

  private void triggeredFlush() {
    try {
      while (bufferCount.get() >= maxBatchSize && !writerPool.isShutdown()) {
        if (flushDB() == 0) {
          triggerBackoffUntil = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(maxBatchAge);
          break;
        }
      }
    } finally {
      pendingFlushes.decrementAndGet();
    }
  }

  // Log a warning about points dropped because the buffer was full, at most once per
  // DROP_WARNING_INTERVAL_NS (or immediately if force is set, e.g. at shutdown)

  private void warnDroppedPoints(boolean force) {
    long now = System.nanoTime();
    long last = lastDropWarning.get();
    if (!force && now - last < DROP_WARNING_INTERVAL_NS) return;
    if (!lastDropWarning.compareAndSet(last, now)) return;   // another thread is reporting

    long dropped = droppedPoints.getAndSet(0);
    if (dropped > 0) {
      // force is only set by the shutdown drain, so label the message accordingly
      itsLogger.warn((force ? "shutdown" : "enqueue") + ": Buffer full (PsqlMaxBufferSize=" + maxBufferSize + "), dropped " + dropped +
                     " oldest points since last report");
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
  // A flush that makes no progress (database unreachable, or batch re-queued after a
  // retryable error) is followed by a short pause before retrying. Note the deadline
  // is checked between attempts, so a slow connection attempt (driver connect timeout)
  // can overrun it somewhat.

  private void drainBuffer() {
    // Report any buffer-full drops not yet logged
    warnDroppedPoints(true);

    long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(shutdownTimeout);
    int initial = bufferCount.get();

    if (initial > 0) {
      itsLogger.info("shutdown: Flushing " + initial + " buffered points to database (timeout " +
                     (shutdownTimeout / 1000) + " s)");
    }

    while (bufferCount.get() > 0 && System.nanoTime() < deadline) {
      if (flushDB() == 0) {
        try {
          Thread.sleep(SHUTDOWN_RETRY_PAUSE_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }

    int remaining = bufferCount.get();
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

  // Put points back at the front of the queue, preserving their order.
  // Returns the number of points re-queued.
  private int requeue(List<PostgresBatchPoint> points) {
    for (int i = points.size() - 1; i >= 0; i--) {
      buffer.addFirst(points.get(i));
      bufferCount.incrementAndGet();
    }
    return points.size();
  }

  // Write a batch of data to the postgres database. Returns the number of points that
  // were put back on the queue to retry later (0 if all were written or discarded).

  private int writeBatch(Connection conn, List<PostgresBatchPoint> batch) {
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
      return 0;
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
        return requeue(batch);
      } else if (batch.size() > 1 && isRowLevel(e)) {
        // One (or a few) bad rows caused the whole batch to be rolled back. Write the rows
        // individually so that only the offending ones are lost.
        itsLogger.warn("insertData: Batch of " + batch.size() + " rejected (SQLState " + state +
                       "), retrying row by row: " + e.getMessage());
        return writeRowByRow(conn, batch);
      } else {
        itsLogger.error("insertData: Discarding " + batch.size() + " points, non-retryable SQL error (SQLState " +
                        state + "): " + e.getMessage());
        return 0;
      }
    }
  }

  // Fallback path: insert each row in its own transaction, dropping (and logging) only the
  // rows the database rejects. If the database becomes unavailable part way through, the
  // current row and everything after it is put back on the queue. Returns the number of
  // points re-queued.

  private int writeRowByRow(Connection conn, List<PostgresBatchPoint> batch) {
    int written = 0;
    int dropped = 0;
    int requeued = 0;

    try {
      conn.setAutoCommit(true);
    } catch (SQLException e) {
      itsLogger.error("insertData: Could not enable autocommit for row-by-row retry, re-adding " +
                      batch.size() + " points to queue: " + e.getMessage());
      return requeue(batch);
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
            requeued = requeue(rest);
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
      return requeue(batch);
    }

    if (dropped > MAX_ROW_ERRORS_LOGGED) {
      itsLogger.error("insertData: ... " + (dropped - MAX_ROW_ERRORS_LOGGED) + " further rejected rows not logged");
    }
    itsLogger.info("insertData: Row-by-row retry wrote " + written + ", dropped " + dropped + ", re-queued " +
                   requeued + " of " + batch.size() + " points");
    return requeued;
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

  // Prepare and write a batch of up to ${maxBatchSize} points into the database. Called
  // by the scheduler, the triggered-flush tasks and the shutdown drain.
  //
  // Returns the number of points permanently removed from the buffer (written, or
  // discarded as unwritable). 0 means no progress: the buffer was empty, the database was
  // unreachable, or the whole batch was re-queued after a retryable error.

  private int flushDB() {
    List<PostgresBatchPoint> batch = new ArrayList<>(maxBatchSize);

    try (Connection conn = dataSource.getConnection()) {
      // Explicitly check whether the connection we have is valid. Before we do any
      // manipulation of the buffer.

      if (conn == null || !conn.isValid(5)) {
        itsLogger.error("flushDB: Database connection invalid");
        return 0;
      }

      while (batch.size() < maxBatchSize) {
        PostgresBatchPoint item = buffer.pollFirst();
        if (item == null) break;
        bufferCount.decrementAndGet();
        batch.add(item);
      }

      if (!batch.isEmpty()) {
        return batch.size() - writeBatch(conn, batch);
      }
      return 0;

    } catch (SQLException e) {
      // If get an error getting a new database connection, return early without touching
      // the buffer at all. This should hopefully make us fault tolerant to intermittent
      // database communication issues.

      itsLogger.error("flushDB: Error creating database connection: " + e.getMessage());
      return 0;
    }
  }
}
