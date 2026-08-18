// Copyright (C) CSIRO Australia Telescope National Facility
//
// This library is free software; you can redistribute it and/or
// modify it under the terms of the GNU Library General Public License
// as published by the Free Software Foundation; either version 2
// of the License, or (at your option) any later version.

package atnf.atoms.mon.archiver;

import java.sql.*;
import java.util.*;
import java.time.*;
import java.math.*;

import javax.naming.*;
import javax.sql.*;

import com.sun.jdi.DoubleValue;
import com.sun.jdi.IntegerValue;
import com.sun.jdi.LongValue;

import atnf.atoms.mon.*;
import atnf.atoms.util.*;
import atnf.atoms.time.*;
import atnf.atoms.mon.util.MonitorConfig;

import org.postgresql.ds.PGPoolingDataSource;
import org.apache.log4j.Logger;
import atnf.atoms.mon.archiver.postgresql.*;

/**
 * Archiver which uses a MySQL database as the back end.
 *
 * <P>
 * Connects to mysqld on localhost as user <i>monica</i> with a blank password. Writes data to a database called <i>MoniCA</i>, so
 * the user needs full permissions to that database. A script <i>bin/setupMySQL.sh</i> is provided which creates the user and the
 * database.
 *
 * <P>
 * TODO: The URL to connect to the database should really be passed as an argument from the configuration file.
 *
 * <P>
 * Since monitor points in MoniCA are not strictly-typed data is stored as a string/varchar and the data type stored in a separate
 * column. The appropriate object is instanciated when data is extracted from the archive, however this approach degrades space
 * efficiency.
 *
 * @author David Brodrick
 */
public class PointArchiverPostgresql extends PointArchiver {
  /**
    * Postgres database name
    */
  private final static String theirDatabase = MonitorConfig.getProperty("PsqlDatabase", "testing");

  /**
    * The name of the postgres database server.
    */
  private final static String theirServer = MonitorConfig.getProperty("PsqlServer", "localhost");

  /**
    * The port to use when connecting to the server.
    */
  private final static String theirPort = MonitorConfig.getProperty("PsqlPort", "5432");

  /**
    * Postgres username
    */
  private final static String theirUsername = MonitorConfig.getProperty("PsqlUsername", "admin");

  /**
    * Postgres password
    */
  private final static String theirPassword = MonitorConfig.getProperty("PsqlPassword", "admin");

  /**
   * Postgres batch size when writing. Potentially parse from config
   */
  private static int theirMaxBatchSize = 500;

  /**
   * Postgres max batch age before writing to db. Potentially parse from config
   */
  private static int theirMaxBatchAge = 10000;

  private String itsURL = null;
  /** The URL to connect to the server/database. */
  //protected String itsURL = "jdbc:mysql://localhost:3306/MoniCA?user=monica&tcpRcvBuf=100000&autoReconnect=true";
  private PGPoolingDataSource itsPgPool;
  private PostgresBatchWriter itsWriter;

  // Hold a mapping between point data and the point_id field for quick writes into
  // the "archive" table
  private HashMap<PostgresPointDesc, Integer> itsPgPointMap = new HashMap<>();

  /* Static block to parse parameters. */
  static {
    try {
      theirMaxBatchSize = Integer.parseInt(MonitorConfig.getProperty("PsqlMaxBatchSize", "500"));
    } catch (Exception e) {
      Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing PsqlMaxBatchSize configuration parameter: " + e);
    }

    try {
      theirMaxBatchAge = 1000 * Integer.parseInt(MonitorConfig.getProperty("PsqlMaxBatchAge", "10"));
    } catch (Exception e) {
      Logger.getLogger(PointArchiver.class.getName()).warn("Error parsing PsqlMaxBatchTime configuration parameter: " + e);
    }
  }

  /** Constructor. */
  public PointArchiverPostgresql() {
    super();

    /* Create a URL string incorporating the options set in the config file */
    itsURL = "jdbc:postgresql://" + theirServer + ":" + theirPort + "/" + theirDatabase + "?user=" + theirUsername + "&password=" + theirPassword + "?reWriteBatchedInserts=true";

    try {
      // Instantiate a postgres server pool so that we can use it here and also
      // in the batched writer

      itsPgPool = new PGPoolingDataSource();
      itsPgPool.setDataSourceName("MonicaArchiveWriterPool");
      itsPgPool.setServerName(theirServer);
      itsPgPool.setDatabaseName(theirDatabase);
      itsPgPool.setUser(theirUsername);
      itsPgPool.setPassword(theirPassword);
      itsPgPool.setMaxConnections(5);

    } catch (Exception e) {
      itsLogger.error("PointArchiverPostgresql Constructor: " + e.getMessage());
    }

    // Try and create all the tables we need when we first connect
    createTables();

    // Configure and start the batch writer process
    itsWriter = new PostgresBatchWriter(itsPgPool, theirMaxBatchAge, theirMaxBatchSize);
    itsWriter.start();
  }

  /**
   * Purge all data for the given point that is older than the specified age in days.
   *
   * @param point
   *          The point whos data we wish to purge.
   */
  protected void purgeOldData(PointDescription point) {
    if (point.getArchiveLongevity() < 0)
      return;

    // Calculate timestamps for period to purge over
    AbsTime start = AbsTime.factory(0);
    AbsTime end = AbsTime.factory((new AbsTime()).getValue() - 86400000000l * point.getArchiveLongevity());
      
    // We can't fully describe one of our "points" from the PointDescription class,
    // this means this function will be a somewhat blunt instrument. It will (by necessity)
    // delete data of multiple "types" as long as the name & source (& units?) match

    String sql = "DELETE FROM archive USING points " +
                 "WHERE " +
                 "(archive.point_id = points.id) AND " +
                 "(name = ? AND units = ? AND source = ? AND ts >= ? AND ts <= ?)";

    try (Connection conn = itsPgPool.getConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
      pstmt.setString(1, point.getName());
      pstmt.setString(2, point.getUnits());
      pstmt.setString(3, point.getSource());
      pstmt.setObject(4, start.getAsDate().toInstant().atOffset(ZoneOffset.UTC));
      pstmt.setObject(5, end.getAsDate().toInstant().atOffset(ZoneOffset.UTC));

      int rows = pstmt.executeUpdate();
      itsLogger.debug("purgeOldData: Successfully purged " + rows + " of data from: " + point.getName());
    } catch (SQLException e) {
      itsLogger.error("purgeOldData: Error purging data from database: " + e.getMessage());
    }
    
    return;
  }

 /**
  * Main loop for the archiving thread.
  *
  * OVERRIDE FROM ARCHIVER CLASS DUE TO NEED
  * FOR ARCHIVING TO INFLUXDB IN REALTIME.
  */
  public void run() {
    setName("Point Archiver");

    RelTime sleeptime1 = RelTime.factory(25000);
    RelTime sleeptime2 = RelTime.factory(500);

    while (true) {
      boolean flushing = false;
      if (itsShuttingDown) {
        flushing = true;
      }

      int counter = 0;
      Enumeration<PointDescription> keys = itsBuffer.keys();
      try {
        while (keys.hasMoreElements()) {
          PointDescription pm = keys.nextElement();
          if (pm == null) {
            continue;
          }

          Vector<PointData> thisdata = itsBuffer.get(pm);
          if (thisdata == null || thisdata.isEmpty()) {
            // No data to be archived
            continue;
          }

          // Process this point data and queue for writing
          saveNow(pm, thisdata);

          try {
            sleeptime2.sleep();
          } catch (Exception e) {
            itsLogger.warn("exception caught: " + e);
          }
          counter++;
        }
      } catch (Exception e) {
        itsLogger.error("While archiving: " + e);
        e.printStackTrace();
      }
      // if (counter > 0) {
      // itsLogger.debug("###### Archived/flagged " + counter + " points");
      // }
      if (itsShuttingDown) {
        // Signal our postgres writer to shutdown
        itsWriter.shutdown();
        itsFlushComplete = true;
        break;
      }
      try {
        sleeptime1.sleep();
      } catch (Exception e) {
        itsLogger.warn("exception caught: " + e);
      }
    }
    itsLogger.info("done shutting down");
  }
  /**
   * Method to do the actual archiving.
   *
   * @param pm
   *          The point whos data we wish to archive.
   * @param data
   *          Vector of data to be archived.
   */
  protected void saveNow(PointDescription pm, Vector<PointData> alldata) {
    // Keep track of the points we have successfully queued
    List<Integer> saved = new ArrayList<>();

    synchronized (alldata) {
        itsLogger.debug("saveNow: Processing " + alldata.size() + " data values for archival from: " + pm.getName());

        // Cycle through all the pointdata for this point, queue for writing
        for (int i = 0; i < alldata.size(); i++) {
          PointData data = (PointData) alldata.get(i);

          // Convert the data point to a "postgres" data representation
          HashMap<String, Object> r = getDatabaseValue(data.getData());

          // What happens if either of these come back null?
          String data_type = (String) r.get("type");
          String value_column = (String) r.get("column");
          Object v = r.get("value");
          String source = null;
          
          // Get (or create) the point_id
          Integer point_id = getPointID(pm.getName(), data_type, pm.getUnits());

          if (point_id != null) {
            if (data.getName() == null || data.getName().isEmpty()) {
              // Get the source from the point description if it's not configured for the
              // point data itself.
              source = pm.getSource();
            } else {
              source = data.getSource();
            }
            
            // Create a batch point ready to be written to the write queue
            PostgresBatchPoint p = new PostgresBatchPoint(
              data.getTimestamp().getAsDate().toInstant().atOffset(ZoneOffset.UTC),
              data.getTimestamp().getValue(),
              point_id,
              source,
              v
            );
  
            // Queue point for writing by postgres writer thread
            itsWriter.enqueue(p);
            // Add this to the list containing indexes of data succesfully saved
            saved.add(i);
          } else {
            itsLogger.error("saveNow: Couldn't get valid point_id for point " + pm.getName());
          }
        }

        itsLogger.debug("saveNow: Successfully queued " + saved.size() + " data values for archival from: " + pm.getName());

        // Clear the data we have successfully queued
        saved.sort(Collections.reverseOrder());
        
        for (int index : saved) {
          alldata.remove(index);
        }
    }
  }

  /**
   * Extract data from the archive.
   *
   * @param pm
   *          Point to extract data for.
   * @param start
   *          Earliest time in the range of interest.
   * @param end
   *          Most recent time in the range of interest.
   * @return Vector containing all data for the point over the time range.
   */
  protected Vector<PointData> extractDeep(PointDescription pm, AbsTime start, AbsTime end) {
    // Build and execute the data request
    String sql = "SELECT * FROM points INNER JOIN archive ON points.id = archive.point_id " +
                 "WHERE name = ? AND source = ? AND (ts >= ? AND ts <= ?) " +
                 "ORDER BY ts " +
                 "LIMIT ?";

    try (Connection conn = itsPgPool.getConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
      // Can't do anything if the server is not running
      if (!checkConnection(conn)) return null;

      // Allocate result vector
      Vector<PointData> res = new Vector<PointData>(1000, 8000);

      // Prepare the "administrative" field values for this data point
      pstmt.setString(1, pm.getName());
      pstmt.setString(2, pm.getSource());
      pstmt.setObject(3, start.getAsDate().toInstant().atOffset(ZoneOffset.UTC));
      pstmt.setObject(4, end.getAsDate().toInstant().atOffset(ZoneOffset.UTC));
      pstmt.setInt(5, MAXNUMRECORDS);

      itsLogger.debug("extractDeep: " + pstmt.toString());

      try (ResultSet rs = pstmt.executeQuery()) {
        // Ensure we got some data
        // if (!rs.first()) return null;

        // Loop through the data, convert to monica points
        while (rs.next()) {
          PointData pd = getPointDataForRow(pm, rs);
          if (pd != null) {
            res.add(pd);
          }
        }
      }

      return res;
    } catch (Exception e) {
      itsLogger.warn("extractDeep: " + e);
      return null;
    }
  }

  /**
   * Return the last update which precedes the specified time. We interpret 'precedes' to mean data_time<=req_time.
   *
   * @param pm
   *          Point to extract data for.
   * @param ts
   *          Find data preceding this timestamp.
   * @return PointData for preceding update or null if none found.
   */
  protected PointData getPrecedingDeep(PointDescription pm, AbsTime ts) {

    String sql = "SELECT * FROM points INNER JOIN archive ON points.id = archive.point_id " +
                 "WHERE name = ? AND source = ? AND ts <= ? " +
                 "ORDER BY ts " +
                 "LIMIT 1";

    try (Connection conn = itsPgPool.getConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
      // Can't do anything if the server is not running
      if (!checkConnection(conn)) return null;

      PointData res = null;

      // Prepare the "administrative" field values for this data point
      pstmt.setString(1, pm.getName());
      pstmt.setString(2, pm.getSource());
      pstmt.setObject(3, ts.getAsDate().toInstant().atOffset(ZoneOffset.UTC));

      itsLogger.debug("getPrecedingDeep: " + pstmt.toString());

      try (ResultSet rs = pstmt.executeQuery()) {
        // There should only ever be a max of one result here
        while (rs.next()) {
          res = getPointDataForRow(pm, rs);
        }
      }

      // Finished - return the extracted data
      return res;
    } catch (Exception e) {
      itsLogger.warn("getPrecedingDeep: " + e);
      return null;
    }
  }

  /**
   * Return the first update which follows the specified time. We interpret 'follows' to mean data_time>=req_time.
   *
   * @param pm
   *          Point to extract data for.
   * @param ts
   *          Find data following this timestamp.
   * @return PointData for following update or null if none found.
   */
  protected PointData getFollowingDeep(PointDescription pm, AbsTime ts) {
    // Build and execute the data request
    String sql = "SELECT * FROM points INNER JOIN archive ON points.id = archive.point_id " +
                 "WHERE name = ? AND source = ? AND ts >= ? " +
                 "ORDER BY ts " +
                 "LIMIT 1";
    
    try (Connection conn = itsPgPool.getConnection(); PreparedStatement pstmt = conn.prepareStatement(sql)) {
      // Can't do anything if the server is not running
      if (!checkConnection(conn)) return null;

      PointData res = null;

      // Prepare the "administrative" field values for this data point

      pstmt.setString(1, pm.getName());
      pstmt.setString(2, pm.getSource());
      pstmt.setObject(3, ts.getAsDate().toInstant().atOffset(ZoneOffset.UTC));

      itsLogger.debug("getFollowingDeep: " + pstmt.toString());

      try (ResultSet rs = pstmt.executeQuery()) {
        // There should only ever be at most one result here
        while (rs.next()) {
          res = getPointDataForRow(pm, rs);
        }
      }

      // Finished - return the extracted data
      return res;
    } catch (Exception e) {
      itsLogger.warn("getPrecedingDeep: " + e);
      return null;
    }
  }

  // Try and get a pointID out of the in memory map. If it's not in the
  // in memory map, see if it's in the database. If it's not in the database
  // yet, add an entry in the "points" table.

  private Integer getPointID(String name, String dataType, String units) {
    PostgresPointDesc pgpd = new PostgresPointDesc(name, dataType, units);
    Integer point_id = itsPgPointMap.get(pgpd);
    String sql = null;

    // If we have a non-null value, then return it and don't do anything else
    if (point_id != null) return point_id;

    itsLogger.debug(String.format("getPointID: Point ('%s', '%s', '%s') not found in in-memory map, querying database", name, dataType, units));

    try (Connection conn = itsPgPool.getConnection()) {

      if (!checkConnection(conn)) {
        itsLogger.error("getPointID: Database connection invalid");
        return null;
      }
      
      // First query the database to see if the point already exits
      sql = "SELECT id FROM points WHERE name = ? AND units = ? AND type = ? " +
            "ORDER BY id LIMIT 1";
      
      try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
        pstmt.setString(1, name);
        pstmt.setString(2, units);
        pstmt.setString(3, dataType);
  
        try (ResultSet rs = pstmt.executeQuery()) {
          while (rs.next()) {
            itsPgPointMap.put(pgpd, rs.getInt("id"));
          }
        }
      } catch (SQLException e) {
        itsLogger.error("getPointID: Error querying database for point_id: " + e.getMessage());
        return null;
      }

      // Try again
      point_id = itsPgPointMap.get(pgpd);
      if (point_id != null) return point_id;

      itsLogger.debug(String.format("getPointID: Point ('%s', '%s', '%s') not found in database, inserting", name, dataType, units));
      
      sql = "INSERT INTO points (name, units, type) " +
            "VALUES (?, ?, ?) " +
            "RETURNING id";

      try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
        pstmt.setString(1, name);
        pstmt.setString(2, units);
        pstmt.setString(3, dataType);
  
        try (ResultSet rs = pstmt.executeQuery()) {
          while (rs.next()) {
            itsPgPointMap.put(pgpd, rs.getInt("id"));
          }
        }
      } catch (SQLException e) {
        itsLogger.error("getPointID: Error inserting new point in database: " + e.getMessage());
        return null;
      }
      
      // This time we return the final value
      point_id = itsPgPointMap.get(pgpd);    
    } catch (SQLException e) {
      itsLogger.error("getPointID: SQL Error looking up or creating new point_id: " + e.getMessage());
      point_id = null;
    }

    return point_id;
  }

  /**
   * Build a PointData from the database row.
   *
   * @param pm
   *          Point the data belongs to.
   * @param rs
   *          The database record/ResultSet.
   * @return PointData representing the data. null if error.
   */
  protected PointData getPointDataForRow(PointDescription pm, ResultSet rs) {
    PointData res = null;

    try {
      AbsTime ts = AbsTime.factory(rs.getLong("bat"));
      String type = rs.getString("type");
      Object val = null;

      // Grab the correct data based on the value in the "type" column.
      // I may simplify this later by using custom views for each type of point.
      switch(type) {
        case "abst":
          val = AbsTime.factory(rs.getLong("val_bigint"));
          break;
        case "ang":
          val = Angle.factory(rs.getDouble("val_float"));
          break;
        case "big":
          val = new BigInteger(rs.getString("val_string"));
          break;
        case "dbl":
          val = rs.getDouble("val_float");
          break;
        case "enum":
          val = EnumItem.valueOf(rs.getString("val_string"));
          break;
        case "flt":
          val = (float) rs.getDouble("val_float");
          break;
        case "hr":
          val = new HourAngle(rs.getDouble("val_float"));
          break;          
        case "int":
          val = rs.getInt("val_int");
          break;
        case "long":
          val = rs.getLong("val_bigint");
          break;
        case "relt":
          val = RelTime.factory(rs.getLong("val_bigint"));
        case "short":
          val = (short) rs.getInt("val_int");
          break;
        case "bool":
          val = rs.getBoolean("val_bool");
          break;
        case "str":
          val = rs.getString("val_string");
          break;
      }

      res = new PointData(pm.getFullName(), ts, val);
    } catch (Exception e) {
      res = null;
    }
    return res;
  }

  /**
   * Create tables required by this archiver. Try it once at startup, if that fails then we
   * assume that the tables are already created
   */

  protected void createTables() {
    try (Connection conn = itsPgPool.getConnection(); Statement stmt = conn.createStatement()) {

      if (!checkConnection(conn)) {
        itsLogger.error("createTables: Database connection invalid, continuing");
        return;
      }

      itsLogger.debug("createTables: Creating point table");

      // Create the table which holds our point metadata
      stmt.execute( "CREATE table IF NOT EXISTS points ("+
                    "id SERIAL PRIMARY KEY, "+
                    "name VARCHAR(100), " +
                    "units VARCHAR(20), " +
                    "type VARCHAR(10), " +
                    "UNIQUE(name, units, type))"
      );

      // Create the table which holds our archive data.
      // TODO: Add unique point_id, ts, source condition?
      stmt.execute( "CREATE table IF NOT EXISTS archive (" +
                    "ts TIMESTAMPTZ, " +
                    "BAT BIGINT, " +
                    "source VARCHAR(20), " +
                    "point_id INTEGER REFERENCES points(id), " +
                    "val_float DOUBLE PRECISION, " + 
                    "val_bigint BIGINT, " +
                    "val_int INTEGER, " +
                    "val_string VARCHAR(255), " +
                    "val_bool BOOLEAN)"
      );
    } catch (Exception e) {
      itsLogger.error("createTables: " + e);
    }
  }

  /**
   * Check if we are connected to the server and reconnect if required.
   *
   * @return True if connected (or reconnected). False if not connected.
   */
  protected boolean checkConnection(Connection conn) {
    // Explicitly check database connection status, return early if invalid
    try {
      if (conn == null || !conn.isValid(5)) {
        return false;
      } else {
        return true;
      }
    } catch (Exception e) {
      itsLogger.warn("checkConnection: Error caught " + e.getMessage());
      return false;
    }
  }

  // Take the data type of the data object and return a string representing the
  // original data type, and also the name of the column we will store it in
  // in the database archive table.

  protected HashMap<String, Object> getDatabaseValue(Object data) {

    HashMap<String, Object> r = new HashMap<>();
  
    String type = null;
    String column = null;
    Object value = null;

    if (data instanceof AbsTime) {
      column = "val_bigint";
      type = "abst";
      value = ((AbsTime) data).getValue();
    } else if (data instanceof Angle) {
      column = "val_float";
      type = "ang";
      value = ((Angle) data).getValue();
    } else if (data instanceof BigInteger) {
      // This may be better as another column entirely (numeric)
      // but for now stick to what was done for InfluxDB
      column = "val_string";
      type = "big";
      value = ((BigInteger) data).toString();
    } else if (data instanceof Double) {
      column = "val_float";
      type = "dbl";
      value = ((Double) data).doubleValue();
    } else if (data instanceof EnumItem) {
      column = "val_string";
      type = "enum";
      value = ((String) data).toString();
    } else if (data instanceof Float) {
      column = "val_float";
      type = "flt";
      value = ((Float) data).doubleValue();
    } else if (data instanceof Integer) {
      column = "val_int";
      type = "int";
      value = ((Integer) data).intValue();
    } else if (data instanceof HourAngle) {
      column = "val_float";
      type = "hr";
      value = ((HourAngle) data).getValue();
    } else if (data instanceof Long) {
      column = "val_bigint";
      type = "long";
      value = ((Long) data).longValue();
    } else if (data instanceof RelTime){
      column = "val_bigint";
      type = "relt";
      value = ((Long) data).longValue();
    } else if (data instanceof Short) {
      column = "val_int";
      type = "short";
      value = ((Integer) data).intValue();
    } else if (data instanceof Boolean) {
      column = "val_bool";
      type = "bool";
      value = ((Boolean) data).booleanValue();
    } else if (data instanceof String) {
      column = "val_string";
      type = "str";
      value = ((String) data);
    }

    // Only return if we have values for all three fields
    if (column != null && type != null && value != null) {
      r.put("column", column);
      r.put("type", type);
      r.put("value", value);
    } else {
      itsLogger.error(String.format("getDatabaseValue: null invalid in return value(s) {'column': %s, 'type': %s, 'value': %s}", column, type, value));
    }

    return r;
  }
}
