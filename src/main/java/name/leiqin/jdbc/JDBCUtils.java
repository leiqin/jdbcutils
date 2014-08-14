package name.leiqin.jdbc;

import java.beans.BeanInfo;
import java.beans.IntrospectionException;
import java.beans.Introspector;
import java.beans.PropertyDescriptor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import javax.sql.DataSource;

public class JDBCUtils {

	protected static int defaultBatchSize = 100;

	protected int batchSize = defaultBatchSize;

	protected DataSource dataSource;

	public JDBCUtils(DataSource dataSource) {
		this.dataSource = dataSource;
	}
	
	public static void setDefaultBatchSize(int defaultBatchSize) {
		JDBCUtils.defaultBatchSize = defaultBatchSize;
	}

	public void setBatchSizez(int batchSize) {
		this.batchSize = batchSize;
	}

	public void batch(String sql, Iterable<?> params,
			String... propertyNames) throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			batch(conn, sql, params, propertyNames);
		} finally {
			close(conn);
		}
	}

	public static void batch(Connection conn, String sql, Iterable<?> params,
			String... propertyNames) throws SQLException {
		List<Object[]> obs = new ArrayList<>();
		int count = 0;
		for (Object bean : params) {
			Object[] ps = params(bean, propertyNames);
			obs.add(ps);
			count++;
			if (count >= defaultBatchSize) {
				batch(conn, sql, obs);
				obs = new ArrayList<>();
				count = 0;
			}
		}
		if (!obs.isEmpty()) {
			batch(conn, sql, obs);
		}
	}

	public int[] batch(String sql, List<Object[]> params)
			throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			return batch(conn, sql, params);
		} finally {
			close(conn);
		}
	}

	public static int[] batch(Connection conn, String sql, List<Object[]> params)
			throws SQLException {
		return batch(conn, sql, params.toArray(new Object[0][0]));
	}

	public int[] batch(String sql, Object[][] params)
			throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			return batch(conn, sql, params);
		} finally {
			close(conn);
		}
	}

	public static int[] batch(Connection conn, String sql, Object[][] params)
			throws SQLException {
		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		if (params == null) {
			throw new SQLException(
					"Null parameters. If parameters aren't need, pass an empty array.");
		}

		PreparedStatement stmt = null;
		int[] rows = null;
		try {
			stmt = conn.prepareStatement(sql);

			for (int i = 0; i < params.length; i++) {
				fillStatement(stmt, params[i]);
				stmt.addBatch();
			}
			rows = stmt.executeBatch();
		} catch (SQLException e) {
			rethrow(e, sql, (Object[]) params);
		} finally {
			close(stmt);
		}

		return rows;
	}

	public int insert(String sql, Object... params)
			throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			return insert(conn, sql, params);
		} finally {
			close(conn);
		}
	}

	public static int insert(Connection conn, String sql, Object... params)
			throws SQLException {
		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		PreparedStatement stmt = null;
		int rows = 0;

		try {
			stmt = conn.prepareStatement(sql);
			fillStatement(stmt, params);
			rows = stmt.executeUpdate();

		} catch (SQLException e) {
			rethrow(e, sql, params);

		} finally {
			close(stmt);
		}

		return rows;
	}

	public int update(String sql, Object... params)
			throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			return update(conn, sql, params);
		} finally {
			close(conn);
		}
	}

	public static int update(Connection conn, String sql, Object... params)
			throws SQLException {
		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		PreparedStatement stmt = null;
		int rows = 0;

		try {
			stmt = conn.prepareStatement(sql);
			fillStatement(stmt, params);
			rows = stmt.executeUpdate();

		} catch (SQLException e) {
			rethrow(e, sql, params);

		} finally {
			close(stmt);
		}

		return rows;
	}

	public static ResultSet queryForResultSet(Connection conn, String sql,
			Object... params) throws SQLException {
		if (conn == null) {
			throw new SQLException("Null connection");
		}

		if (sql == null) {
			throw new SQLException("Null SQL statement");
		}

		PreparedStatement stmt = null;
		ResultSet rs = null;

		try {
			stmt = conn.prepareStatement(sql);
			fillStatement(stmt, params);
			rs = stmt.executeQuery();

			return rs;
		} catch (SQLException e) {
			rethrow(e, sql, params);
			return null;
		}

	}

	public Map<String, Object> queryOne(String sql, 
			Object... params) throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			return queryOne(conn, sql, params);
		} finally {
			close(conn);
		}
	}

	public static Map<String, Object> queryOne(Connection conn, String sql, 
			Object... params) throws SQLException {
		Map<String, Object> result = null;
		for (Map<String, Object> map : query(conn, sql, params)) {
			result = map;
			break;
		}
		return result;
	}

	public static Iterable<Map<String, Object>> query(Connection conn, String sql,
			Object... params) throws SQLException {
		ResultSet rs = queryForResultSet(conn, sql, params);
		return MapIterator.iterable(rs);
	}

	public <T> T queryOne(Class<T> cla, String sql, 
			Object... params) throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			return queryOne(cla, conn, sql, params);
		} finally {
			close(conn);
		}
	}

	public static <T> T queryOne(Class<T> cla, Connection conn, String sql, 
			Object... params) throws SQLException {
		T result = null;
		for (T t : query(cla, conn, sql, params)) {
			result = t;
			break;
		}
		return result;
	}

	public static <T> Iterable<T> query(Class<T> cla, Connection conn, String sql,
			Object... params) throws SQLException {
		ResultSet rs = queryForResultSet(conn, sql, params);
		Map<String, String> columnToPropertyOverrides = getClassColumnToPropertyOverrides(cla);
		return BeanIterator.iterable(rs, cla, columnToPropertyOverrides);
	}

	protected static Map<Class<?>, Map<String, String>> classColumnToPropertyOverrides
		 = new ConcurrentHashMap<>();

	protected static Map<String, String> getClassColumnToPropertyOverrides(Class<?> cla) {
		Map<String, String> result = classColumnToPropertyOverrides.get(cla);
		if (result != null)
			return result;
		try {
			result = new HashMap<>();
			do {
				Field[] fs = cla.getDeclaredFields();
				for (Field f : fs) {
					ColumnName cn = f.getAnnotation(ColumnName.class);
					if (cn != null) {
						result.put(cn.value(), f.getName());
					}
				}
				cla = cla.getSuperclass();
			} while (cla != Object.class);
			classColumnToPropertyOverrides.put(cla, result);
			return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public <T> T queryOne(Class<T> cla, 
			Map<String, String> columnToPropertyOverrides,
			String sql, Object... params) throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			return queryOne(cla, columnToPropertyOverrides, conn, sql, params);
		} finally {
			close(conn);
		}
	}

	public static <T> T queryOne(Class<T> cla, 
			Map<String, String> columnToPropertyOverrides,
			Connection conn, String sql, 
			Object... params) throws SQLException {
		T result = null;
		for (T t : query(cla, columnToPropertyOverrides, conn, sql, params)) {
			result = t;
			break;
		}
		return result;
	}

	public static <T> Iterable<T> query(Class<T> cla, 
			Map<String, String> columnToPropertyOverrides,
			Connection conn, String sql, Object... params) throws SQLException {
		ResultSet rs = queryForResultSet(conn, sql, params);
		return BeanIterator.iterable(rs, cla, columnToPropertyOverrides);
	}

	public long count(String sql, Object... params) throws SQLException {
		Connection conn = dataSource.getConnection();
		try {
			return count(conn, sql, params);
		} finally {
			close(conn);
		}
	}

	public static long count(Connection conn, String sql, Object... params) throws SQLException {
		ResultSet rs = queryForResultSet(conn, sql, params);
		try {
			rs.next();
			return rs.getLong(1);
		} finally {
			close(rs);
		}
	}

	public static void fillStatement(PreparedStatement stmt, Object... params)
			throws SQLException {

		// nothing to do here
		if (params == null) {
			return;
		}

		for (int i = 0; i < params.length; i++) {
			if (params[i] != null) {
				stmt.setObject(i + 1, params[i]);
			} else {
				// VARCHAR works with many drivers regardless
				// of the actual column type. Oddly, NULL and
				// OTHER don't work with Oracle's drivers.
				int sqlType = Types.VARCHAR;
				stmt.setNull(i + 1, sqlType);
			}
		}
	}

	/**
	 * Throws a new exception with a more informative error message.
	 *
	 * @param cause
	 *            The original exception that will be chained to the new
	 *            exception when it's rethrown.
	 *
	 * @param sql
	 *            The query that was executing when the exception happened.
	 *
	 * @param params
	 *            The query replacement parameters; <code>null</code> is a valid
	 *            value to pass in.
	 *
	 * @throws SQLException
	 *             if a database access error occurs
	 */
	protected static void rethrow(SQLException cause, String sql,
			Object... params) throws SQLException {

		String causeMessage = cause.getMessage();
		if (causeMessage == null) {
			causeMessage = "";
		}
		StringBuffer msg = new StringBuffer(causeMessage);

		msg.append(" Query: ");
		msg.append(sql);
		msg.append(" Parameters: ");

		if (params == null) {
			msg.append("[]");
		} else {
			msg.append(Arrays.deepToString(params));
		}

		SQLException e = new SQLException(msg.toString(), cause.getSQLState(),
				cause.getErrorCode());
		e.setNextException(cause);

		throw e;
	}

	public static void close(AutoCloseable closeable) {
		try {
			closeable.close();
		} catch (Exception e) {
			// close error ignore
		}
	}

	public static Object[] params(Object bean, String... propertyNames) {
		if (propertyNames == null || propertyNames.length == 0)
			return new Object[0];
		Object[] result = new Object[propertyNames.length];
		try {
			for (int i = 0; i < result.length; i++) {
				result[i] = getProperty(bean, propertyNames[i]);
			}
			return result;
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	protected static Map<Class<?>, Map<String, PropertyDescriptor>> propertyDescriptorMap = new ConcurrentHashMap<>();

	public static PropertyDescriptor getPropertyDescriptor(Object bean,
			String propertyName) throws IntrospectionException {
		Map<String, PropertyDescriptor> map = propertyDescriptorMap.get(bean.getClass());
		if (map == null) {
			BeanInfo bi = Introspector.getBeanInfo(bean.getClass());
			PropertyDescriptor[] pds = bi.getPropertyDescriptors();
			map = new HashMap<>();
			for (PropertyDescriptor pd : pds)
				map.put(pd.getName(), pd);
		}
		PropertyDescriptor pd = map.get(propertyName);
		return pd;
	}

	public static Object getProperty(Object bean, String propertyName)
			throws IntrospectionException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		PropertyDescriptor pd = getPropertyDescriptor(bean, propertyName);
		if (pd == null) {
			throw new IllegalArgumentException("class " + bean.getClass().toString() + " doesn't has property " + propertyName);
		}
		Method m = pd.getReadMethod();
		return m.invoke(bean);
	}

	public static void setProperty(Object bean, String propertyName, Object value)
			throws IntrospectionException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException {
		PropertyDescriptor pd = getPropertyDescriptor(bean, propertyName);
		if (pd == null) {
			throw new IllegalArgumentException("class " + bean.getClass().toString() + " doesn't has property " + propertyName);
		}
		Method m = pd.getWriteMethod();
		m.invoke(bean, value);
	}

	/**
	 * @return the dataSource
	 */
	public DataSource getDataSource() {
		return dataSource;
	}

	/**
	 * @param dataSource the dataSource to set
	 */
	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}
}
