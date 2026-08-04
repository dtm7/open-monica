package atnf.atoms.mon.archiver.postgresql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

import atnf.atoms.mon.archiver.postgresql.PostgresBatchPoint;

public class PostgresBatchWriter {
  private final ConcurrentLinkedDeque<PostgresBatchPoint> buffer = new ConcurrentLinkedDeque<>();

  // Try with modest thread count values to begin with
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
  private final ExecutorService writerPool = Executors.newFixedThreadPool(2);

  private final javax.sql.DataSource dataSource; // configure your data source

  public PostgresBatchWriter(javax.sql.DataSource dataSource) {
    this.dataSource = dataSource;
    // Start background flusher task, flush data every 10s
    scheduler.scheduleAtFixedRate(this::flushBatch, 10000, 10000, TimeUnit.MILLISECONDS);
  }

  public void enqueue(PostgresBatchPoint data) {
    buffer.addLast(data);
    if (buffer.size() >= 500) { // trigger early flush if threshold reached
      writerPool.submit(this::flushBatch);
    }
  }

  private synchronized void flushBatch() {
    List<PostgresBatchPoint> batch = new ArrayList<>(500);
    while (batch.size() < 500 && !buffer.isEmpty()) {
      PostgresBatchPoint item = buffer.pollFirst();
      if (item != null) batch.add(item);
    }

    if (batch.isEmpty()) return;

    writerPool.submit(() -> {

      // Every insert statement should be the same format so the SQL driver can batch all the inserts
      // together for performance
      String sql = "INSERT INTO archive " +
                   "(ts, bat, point_id, source, val_float, val_bigint, val_int, val_string) " +
                   "VALUES (?, ?, ?, ?, ?, ?, ?, ?)";

      // Construct the prepared statement and try and write it
      try (Connection conn = dataSource.getConnection(); PreparedStatement ps = conn.prepareStatement(sql)) {
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

          ps.addBatch();
        }

        ps.executeBatch();
        conn.commit();
      } catch (SQLException e) {
        // handle retry or log error
      }
    });
  }
}
