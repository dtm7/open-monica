package atnf.atoms.mon.archiver.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
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
  private final int maxQueueSize;

  public PostgresBatchWriter(javax.sql.DataSource dataSource, int maxBatchAge, int maxBatchSize, int maxQueueSize) {
    this.dataSource = dataSource;
    this.maxBatchAge = maxBatchAge;
    this.maxBatchSize = maxBatchSize;
    this.maxQueueSize = maxQueueSize;
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
    if (buffer.size() >= maxQueueSize) buffer.pollFirst();

    // Add the new point to the end of the buffer
    buffer.addLast(data);

    // trigger early flush if threshold reached
    if (buffer.size() >= maxBatchSize) writerPool.submit(this::flushDB);
  }

  // Do an orderly shutdown of the postgres batch writer. First stop any existing
  // threads, then do a final (synchronous) database flush.

  public void shutdown() {
    itsLogger.info("shutdown: Shutting down postgres writer threads");
    writerPool.shutdown();
    try {
      // Wait for the batch delay time for any threads to terminate
      if (!writerPool.awaitTermination(maxBatchAge, TimeUnit.MILLISECONDS)) {
        writerPool.shutdownNow();
      }
    } catch (InterruptedException e) {
      writerPool.shutdownNow();
      Thread.currentThread().interrupt();
    }
    // Flush any remaining data on the queue to the database
    flushDB();
  }

  // Write a batch of data to the postgres database
  
  private void writeBatch(Connection conn, List<PostgresBatchPoint> batch) {
    // Every insert statement should be the same format so the SQL driver can batch all the inserts
    // together for performance. We add "on conflict do nothing" so that tripping potential
    // unique constraints (ts, point_id, source) does not lose the whole batch.
    String sql = "INSERT INTO archive " +
                "(ts, bat, point_id, source, val_float, val_bigint, val_int, val_string, val_bool) " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) " +
                "ON CONFLICT DO NOTHING";
    
    int points = 0;
    
    // Construct the prepared statement and try and write it
    try (PreparedStatement ps = conn.prepareStatement(sql)) {
      // Allow the SQL driver to auto batch our inserts
      conn.setAutoCommit(false);
      for (PostgresBatchPoint data : batch) {
        ps.setObject(1, data.timestamp);
        ps.setObject(2, data.bat);
        ps.setObject(3, data.pointid);
        ps.setString(4, data.source);

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
          ps.setString(8, (String) data.value);
        } else {
          ps.setNull(8, java.sql.Types.VARCHAR);
        }

        if (data.value instanceof Boolean) {
          ps.setBoolean(9, (Boolean) data.value);
        } else {
          ps.setNull(9, java.sql.Types.BOOLEAN);
        }

        ps.addBatch();
        points++;
      }
    
      ps.executeBatch();
      itsLogger.debug("insertData: Comitting " + points + " data points to database");
      conn.commit();
    } catch (SQLTransientException | SQLRecoverableException e) {
      itsLogger.error("insertData: Transient/Recoverable SQL exception, re-adding batch to queue: " + e.getMessage());

      // Strictly speaking, the order things get inserted doesn't matter, but add to the start of the queue anyway
      for (PostgresBatchPoint data : batch) {
        buffer.addFirst(data);
      }
  
    } catch (SQLNonTransientException e) {
      itsLogger.error("insertData: Discarding transaction: unhandled Non-Transient SQL exception: " + e.getMessage());
    } catch (SQLException e) {
      itsLogger.error("insertData: Discarding transaction: unhandled SQL exception: " + e.getMessage());
    }
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
