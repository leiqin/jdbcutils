package name.leiqin.jdbc;

import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

public class BeanIterator<T> implements Iterator<T> {

	private final ResultSet rs;
	private final Class<T> cla;
	private final Map<String, String> columnToPropertyOverrides;

	public BeanIterator(ResultSet rs, Class<T> cla,
			Map<String, String> columnToPropertyOverrides) {
		this.rs = rs;
		this.cla = cla;
		this.columnToPropertyOverrides = columnToPropertyOverrides;
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
	public T next() {
		try {
			if (hasNext) {
				hasNext = false;
			} else {
				if (!rs.next())
					throw new NoSuchElementException();
			}
			return toBean(rs);
		} catch (Exception e) {
			rethrow(e);
			return null;
		}
	}

	protected ResultSetMetaData rsmd;

	public T toBean(ResultSet rs) throws SQLException, InstantiationException,
			IllegalAccessException, IntrospectionException,
			IllegalArgumentException, InvocationTargetException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int count = rsmd.getColumnCount();
		T bean = cla.newInstance();
		for (int i = 0; i < count; i++) {
			String name = rsmd.getColumnLabel(i + 1);
			if (name == null || name.trim().length() == 0)
				name = rsmd.getColumnName(i + 1);
			Object value = rs.getObject(i + 1);
			if (value == null)
				continue;
			if (columnToPropertyOverrides.containsKey(name))
				name = columnToPropertyOverrides.get(name);
			PropertyDescriptor pd = JDBCUtils.getPropertyDescriptor(bean, name);
			if (pd == null) {
				// property not find ignore
				continue;
			}

			Class<?> clazz = pd.getPropertyType();
			if (clazz == boolean.class || clazz == Boolean.class) {
				value = rs.getBoolean(i + 1);
			} else if (clazz == int.class || clazz == Integer.class) {
				value = rs.getInt(i + 1);
			} else if (clazz == long.class || clazz == Long.class) {
				value = rs.getLong(i + 1);
			} else if (clazz == double.class || clazz == Double.class) {
				value = rs.getDouble(i + 1);
			} else if (clazz == BigDecimal.class) {
				value = rs.getBigDecimal(i + 1);
			} else if (clazz == Date.class) {
				Timestamp ts = rs.getTimestamp(i + 1);
				value = new Date(ts.getTime());
			} else if (clazz == Calendar.class) {
				Timestamp ts = rs.getTimestamp(i + 1);
				Calendar cal = Calendar.getInstance();
				cal.setTime(ts);
				value = cal;
			}
			JDBCUtils.setProperty(bean, name, value);
		}
		return bean;
	}

    @Override
    public void remove() {
		throw new UnsupportedOperationException();
    }

    protected void rethrow(Exception e) {
        throw new RuntimeException(e);
    }

    public static <T> Iterable<T> iterable(final ResultSet rs, final Class<T> cla) {
        return new Iterable<T>() {

            @Override
            public Iterator<T> iterator() {
                return new BeanIterator<>(rs, cla, new HashMap<String, String>(0));
            }

        };
    }

    public static <T> Iterable<T> iterable(final ResultSet rs, final Class<T> cla, 
			final Map<String, String> columnToPropertyOverrides) {
        return new Iterable<T>() {

            @Override
            public Iterator<T> iterator() {
                return new BeanIterator<>(rs, cla, columnToPropertyOverrides);
            }

        };
    }

}
