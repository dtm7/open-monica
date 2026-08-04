package atnf.atoms.mon.archiver.postgresql;

import java.time.OffsetDateTime;

// Simple class which is intended to hold enough information to fully describe a
// monica data point (including value) so that it can be written correctly into
// a PostgreSQL database.

public class PostgresBatchPoint {

  // Keep it simple, use public fields for each piece of information that we
  // use to write to Postgres.
  public OffsetDateTime timestamp;
  public Long bat;
  public int pointid;
  public String source;

  // This can (currently) be Double, Long, Integer, String.
  public Object value;

  // Constructor
  public PostgresBatchPoint(OffsetDateTime timestamp, Long bat, int pointid, String source, Object value) {
    this.timestamp = timestamp;
    this.bat = bat;
    this.pointid = pointid;
    this.source = source;
    this.value = value;
  }
}
