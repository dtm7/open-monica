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

import javax.naming.*;
import javax.sql.*;

import com.sun.jdi.DoubleValue;
import com.sun.jdi.IntegerValue;
import com.sun.jdi.LongValue;

import atnf.atoms.mon.*;
import atnf.atoms.util.*;
import atnf.atoms.time.*;
import atnf.atoms.mon.util.MonitorConfig;

import atnf.atoms.mon.archiver.postgresql.PostgresBatchWriter;
import atnf.atoms.mon.archiver.postgresql.PostgresBatchPoint;

import org.postgresql.ds.PGPoolingDataSource;

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
  /** The connection to the MySQL server. */
  protected Connection itsConnection = null;

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

  /** Maximum offset to be added to above based on hash of specific point name. */
  protected static final long theirMaxAgeOffset = 0;

  private String itsURL = null;
  /** The URL to connect to the server/database. */
  //protected String itsURL = "jdbc:mysql://localhost:3306/MoniCA?user=monica&tcpRcvBuf=100000&autoReconnect=true";
  private PGPoolingDataSource itsPgPool;
  private PostgresBatchWriter itsWriter;

  /** Constructor. */
  public PointArchiverPostgresql() {
    super();

    /* Create a URL string incorporating the options set in the config file */
    itsURL = "jdbc:postgresql://" + theirServer + ":" + theirPort + "/" + theirDatabase + "?user=" + theirUsername + "&password=" + theirPassword + "?reWriteBatchedInserts=true";

    try {
      // Instantiate a datasource so that we can use it with out postgres writer class

      itsPgPool = new PGPoolingDataSource();
      itsPgPool.setDataSourceName("MonicaArchiveWriterPool");
      itsPgPool.setServerName(theirServer);
      itsPgPool.setDatabaseName(theirDatabase);
      itsPgPool.setUser(theirUsername);
      itsPgPool.setPassword(theirPassword);
      itsPgPool.setMaxConnections(5);

      // Connection for the base object to use for queries, metadata writes
      itsConnection = itsPgPool.getConnection();
    } catch (Exception e) {
      itsLogger.error("PointArchiverPostgresql Constructor: " + e.getMessage());
      itsConnection = null;
    }

    itsWriter = new PostgresBatchWriter(itsPgPool);

    // Try and create all the tables we need when we first connect
    createTables();
  }

  /**
   * Purge all data for the given point that is older than the specified age in days.
   *
   * @param point
   *          The point whos data we wish to purge.
   */
  protected void purgeOldData(PointDescription point) {
    // Implement this later ... In reality we probably don't want to purge
    // old data from postgres
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

          //noinspection StatementWithEmptyBody
          if (!itsShuttingDown) {
            // archive immediately to influx, it will handle batching
            saveNow(pm, thisdata);
          }

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
      if (itsShuttingDown && flushing) {
        // We've just flushed the full archive
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
    // Assuming every point in this batch has the same type
    Map.Entry<String, String> result = getDatabaseValueCol(alldata.get(0).getData());

    // What happens if either of these come back null?
    String data_type = result.getKey();
    String value_column = result.getValue();

    // Insert a new point into the table, and (always) return the results. This will
    // get nicer in postgres 19 with ON CONFLICT SELECT
    String cmd = "INSERT INTO points (name, units, type) " +
                 "VALUES ('" + pm.getName() + "','" + pm.getUnits() + "', '" + data_type + "') " +
                 "ON CONFLICT (name, units, type) " +
                 "DO UPDATE SET name = EXCLUDED.name " +
                 "RETURNING id, name, units, type";

    ResultSet point_insert = null;
    Integer point_id = null;

    try {
      Statement stmt = null;
      synchronized (itsConnection) {
        stmt = itsConnection.createStatement();
        stmt.execute(cmd);

        // Grab the point_id of this point, we will use it when we insert archival data
        point_insert = stmt.getResultSet();

        // There should always be only one result here.
        if (point_insert.next()) {
            point_id = point_insert.getInt("id");
        }

        stmt.close();
      }
    } catch (Exception e) {
      itsLogger.warn("insertData: " + cmd);
      itsLogger.warn("insertData: " + e);
    }

    synchronized (alldata) {

        // Cycle through all the pointdata for this point, queue for writing
        for (int i = 0; i < alldata.size(); i++) {
          PointData data = (PointData) alldata.get(i);

          // Figure out the data type
          Object v = null;
          switch (value_column) {
            case "val_float":
              v = ((Double) data.getData()).doubleValue();
              break;
            case "val_bigint":
              v = ((Long) data.getData()).longValue();
              break;
            case "val_int":
              v = ((Integer) data.getData()).intValue();
              break;
            case "val_string":
              v = ((String) data.getData());
              break;
          }

          // Create a batch point ready to be written to the write queue
          PostgresBatchPoint p = new PostgresBatchPoint(
            data.getTimestamp().getAsDate().toInstant().atOffset(ZoneOffset.UTC),
            data.getTimestamp().getValue(),
            point_id,
            data.getSource(),
            v
          );

          itsLogger.warn("insertData: Queueing point for archival: " + pm.getName() + "." + data.getSource());
          // Queue point for writing by postgres writer thread
          itsWriter.enqueue(p);
        }

        // All data queued, clear it
        alldata.clear();
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
    try {
      // Can't do anything if the server is not running
      if (!checkConnection()) {
        return null;
      }

      // Allocate result vector
      Vector<PointData> res = new Vector<PointData>(1000, 8000);

      // Build and execute the data request
      String cmd = "SELECT * FROM points INNER JOIN archive ON points.id = archive.point_id " +
                   "WHERE name = ? AND source = ? AND (ts >= ? AND ts <= ?) " +
                   "ORDER BY ts " +
                   "LIMIT ?";

      synchronized (itsConnection) {
        try (PreparedStatement pstmt = itsConnection.prepareStatement(cmd)) {
          // Prepare the "administrative" field values for this data point
          pstmt.setString(1, pm.getName());
          pstmt.setString(2, pm.getSource());
          pstmt.setObject(3, start.getAsDate().toInstant().atOffset(ZoneOffset.UTC));
          pstmt.setObject(4, end.getAsDate().toInstant().atOffset(ZoneOffset.UTC));
          pstmt.setInt(5, MAXNUMRECORDS);

          itsLogger.warn("extractDeep: " + pstmt.toString());

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
        }
      }

      return res;
    } catch (Exception e) {
      itsLogger.warn("extract: " + e);
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
    try {
      // Can't do anything if the server is not running
      if (!checkConnection()) {
        return null;
      }

      PointData res = null;

      // Build and execute the data request
      String cmd = "SELECT * FROM points INNER JOIN archive ON points.id = archive.point_id " +
                   "WHERE name = ? AND source = ? AND ts <= ? " +
                   "ORDER BY ts " +
                   "LIMIT 1";

      synchronized (itsConnection) {
        try (PreparedStatement pstmt = itsConnection.prepareStatement(cmd)) {
          // Prepare the "administrative" field values for this data point
          pstmt.setString(1, pm.getName());
          pstmt.setString(2, pm.getSource());
          pstmt.setObject(3, ts.getAsDate().toInstant().atOffset(ZoneOffset.UTC));

          itsLogger.warn("getPrecedingDeep: " + pstmt.toString());

          try (ResultSet rs = pstmt.executeQuery()) {
            res = getPointDataForRow(pm, rs);
          }
        }
      }

      // Finished - return the extracted data
      return res;
    } catch (Exception e) {
      itsLogger.warn("getPreceding: " + e);
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
    try {
      // Can't do anything if the server is not running
      if (!checkConnection()) {
        return null;
      }

      PointData res = null;

      // Build and execute the data request
      String cmd = "SELECT * FROM points INNER JOIN archive ON points.id = archive.point_id " +
                   "WHERE name = ? AND source = ? AND ts >= ? " +
                   "ORDER BY ts " +
                   "LIMIT 1";

      synchronized (itsConnection) {
        try (PreparedStatement pstmt = itsConnection.prepareStatement(cmd)) {
          // Prepare the "administrative" field values for this data point
          pstmt.setString(1, pm.getName());
          pstmt.setString(2, pm.getSource());
          pstmt.setObject(3, ts.getAsDate().toInstant().atOffset(ZoneOffset.UTC));

          itsLogger.warn("getFollowingDeep: " + pstmt.toString());

          try (ResultSet rs = pstmt.executeQuery()) {
            res = getPointDataForRow(pm, rs);
          }
        }
      }

      // Finished - return the extracted data
      return res;
    } catch (Exception e) {
      itsLogger.warn("getPreceding: " + e);
      return null;
    }
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
        case "dbl":
          val = rs.getDouble("val_float");
          break;
        case "flt":
          val = (float) rs.getDouble("val_float");
          break;
        case "int":
          val = rs.getInt("val_int");
          break;
        case "long":
          val = rs.getLong("val_bigint");
          break;
        case "short":
          val = (short) rs.getInt("val_int");
          break;
        case "bool":
          val = (rs.getInt("val_int") != 0);
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
   * Create tables required by this archiver
   */

  protected void createTables() {
    Statement stmt = null;
    try {
      itsLogger.debug("createTables: Creating point table");
      synchronized (itsConnection) {
        stmt = itsConnection.createStatement();
        stmt.execute("CREATE table IF NOT EXISTS points (id SERIAL PRIMARY KEY, name VARCHAR(100), units VARCHAR(20), type VARCHAR(10), UNIQUE(name, units, type))");
        stmt.execute("CREATE table IF NOT EXISTS archive (ts TIMESTAMPTZ, BAT BIGINT, source VARCHAR(20), point_id INTEGER REFERENCES points(id), val_float DOUBLE PRECISION, val_bigint BIGINT, val_int INTEGER, val_string VARCHAR(255))");
        stmt.close();
      }
    } catch (Exception e) {
      itsLogger.error("createTables: " + e);
      try {
        if (stmt != null) {
          stmt.close();
        }
      } catch (Exception g) {
      }
    }
  }

  /**
   * Check if we are connected to the server and reconnect if required.
   *
   * @return True if connected (or reconnected). False if not connected.
   */
  protected boolean checkConnection() {
    boolean res = false;
    Statement stmt = null;
    try {
      // Do a minimal query to see if connection is valid
      // From Java 1.6 we could use itsConnection.isValid(1)
      if (itsConnection != null) {
        try {
          synchronized (itsConnection) {
            stmt = itsConnection.createStatement();
            stmt.execute("select 1;");
            stmt.close();
          }
        } catch (Exception f) {
          itsConnection = null;
          try {
            if (stmt != null) {
              stmt.close();
            }
          } catch (Exception g) {
          }
        }
      }
      if (itsConnection == null) {
        itsConnection = DriverManager.getConnection(itsURL);
      } else {
        res = true;
      }
    } catch (Exception e) {
      itsLogger.warn("checkConnection: " + e);
      itsConnection = null;
    }
    return res;
  }

  // Take the data type of the data object and return a string representing the
  // original data type, and also the name of the column we will store it in
  // in the database archive table.

  protected Map.Entry<String, String> getDatabaseValueCol(Object data) {
    String type = null;
    String res = null;

    // Still missing some data types here:
    //  - Angle
    //  - HourAngle
    //  - AbsTime
    //  - RelTime
    //  - EnumItem

    if (data instanceof Double) {
      res = "val_float";
      type = "dbl";
    } else if (data instanceof Float) {
      res = "val_float";
      type = "flt";
    } else if (data instanceof Integer) {
      res = "val_int";
      type = "int";
    } else if (data instanceof Long) {
      res = "val_bigint";
      type = "long";
    } else if (data instanceof Short) {
      res = "val_int";
      type = "short";
    } else if (data instanceof Boolean) {
      res = "val_bool";
      type = "bool";
    } else if (data instanceof String) {
      res = "val_string";
      type = "str";
    }

    return new AbstractMap.SimpleEntry<>(type, res);
  }
}
