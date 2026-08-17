// This class contains all the information required to make an entry into the
// Postgres "point" table. At this point, this is "name", "data type" and "units"
//
// We use this to store the relationship between a "postgres" point and point_id
// in memory so we don't have to query the database for the point_id every time

package atnf.atoms.mon.archiver.postgresql;

import java.util.Objects;

public class PostgresPointDesc {
    private final String name;
    private final String dataType;
    private final String units;

    public PostgresPointDesc(String name, String dataType, String units) {
        this.name = name;
        this.dataType = dataType;
        this.units = units;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        PostgresPointDesc p = (PostgresPointDesc) o;
        return Objects.equals(name, p.name) && Objects.equals(dataType, p.dataType) && Objects.equals(units, p.units);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, dataType, units);
    }
}
