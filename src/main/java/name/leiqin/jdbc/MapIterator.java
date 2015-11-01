package name.leiqin.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class MapIterator implements Iterator<Map<String, Object>> {

    private final ResultSet rs;

    public MapIterator(ResultSet rs) {
        this.rs = rs;
    }

	private boolean hasNext;

    @Override
    public boolean hasNext() {
		if (hasNext) {
			return true;
		} else {
			try {
				if (rs.next()) {
					hasNext = true;
					return true;
				} else {
					return false;
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
		}
    }

    @Override
    public Map<String, Object> next() {
        try {
			if (hasNext) {
				hasNext = false;
			} else {
				if (!rs.next())
					throw new NoSuchElementException();
			}
            return toMap(rs);
        } catch (SQLException e) {
            rethrow(e);
            return null;
        }
    }

	protected ResultSetMetaData rsmd;

	protected Map<String, Object> toMap(ResultSet rs) throws SQLException {
		if (rsmd == null)
			rsmd = rs.getMetaData();
		int count = rsmd.getColumnCount();
		Map<String, Object> result = new LinkedHashMap<>(count);
		for (int i = 0; i < count; i++) {
			String name = rsmd.getColumnLabel(i + 1);
			if (name == null || name.trim().length() == 0)
				name = rsmd.getColumnName(i + 1);
			Object value = rs.getObject(i + 1);
			result.put(name, value);
		}
		return result;
	}

    @Override
    public void remove() {
		throw new UnsupportedOperationException();
    }

    protected void rethrow(SQLException e) {
        throw new RuntimeException(e);
    }

    public static Iterable<Map<String, Object>> iterable(final ResultSet rs) {
        return new Iterable<Map<String, Object>>() {

            @Override
            public Iterator<Map<String, Object>> iterator() {
                return new MapIterator(rs);
            }

        };
    }

}
