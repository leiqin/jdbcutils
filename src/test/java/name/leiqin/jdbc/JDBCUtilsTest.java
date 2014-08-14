package name.leiqin.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class JDBCUtilsTest {

	Connection conn;

	@Before
	public void beforeTest() throws ClassNotFoundException, SQLException {
		Class.forName("org.sqlite.JDBC");
		this.conn = DriverManager.getConnection("jdbc:sqlite::memory:");
		JDBCUtils.update(conn, "DROP TABLE IF EXISTS test_bean");
		JDBCUtils.update(conn, "CREATE TABLE IF NOT EXISTS test_bean (id integer PRIMARY KEY AUTOINCREMENT, name text, age integer, brithday timestamp, hello_world text, data_value real)");
	}

	@After
	public void afterTest() throws SQLException {
		this.conn.close();
	}

	public static TestBean createBean(String name, int age, String helloWorld,
			Date brithday, double value) {
		TestBean testBean = new TestBean();
		testBean.setName(name);
		testBean.setAge(age);
		testBean.setHelloWorld(helloWorld);
		testBean.setBrithday(brithday);
		testBean.setValue(value);
		return testBean;
	}

	public static String INSERT_SQL = "INSERT INTO test_bean (name, age, brithday, hello_world, data_value) VALUES (?, ?, ?, ?, ?)";

	@Test
	public void testInsertAndQuery() throws SQLException {
		Date brithday = new Date();
		TestBean testBean = createBean("Hello world", 18, "Ha Ha Ha",
			   brithday, 1.23);	
		JDBCUtils.insert(conn, INSERT_SQL,
				JDBCUtils.params(testBean, "name", "age", "brithday", "helloWorld", "value"));
		TestBean queryBean = JDBCUtils.queryOne(TestBean.class, conn, "select * from test_bean");
		Assert.assertEquals(testBean.getName(), queryBean.getName());
		Assert.assertEquals(testBean.getAge(), queryBean.getAge());
		Assert.assertEquals(testBean.getHelloWorld(), queryBean.getHelloWorld());
		Assert.assertEquals(testBean.getBrithday(), queryBean.getBrithday());
		Assert.assertEquals(testBean.getValue(), queryBean.getValue(), 0.0001);
	}

	@Test
	public void testBetchAndCount() throws SQLException {
		String[] names = {"AAA", "BBB", "CCC", "DDD", "EEE"};
		List<TestBean> list = new ArrayList<>();
		for (String name : names) {
			TestBean tb = createBean(name, 18, "Hello", new Date(), 1.23);
			list.add(tb);
		}
		JDBCUtils.batch(conn, INSERT_SQL, list, "name", "age", "brithday", "helloWorld", "value");
		
		long count = JDBCUtils.count(conn, "select count(*) from test_bean");
		Assert.assertEquals(count, names.length);
	}

	@Test
	public void testBetchAndQuery() throws SQLException {
		String[] names = {"AAA", "BBB", "CCC", "DDD", "EEE"};
		List<TestBean> list = new ArrayList<>();
		for (String name : names) {
			TestBean tb = createBean(name, 18, "Hello", new Date(), 1.23);
			list.add(tb);
		}
		JDBCUtils.batch(conn, INSERT_SQL, list, "name", "age", "brithday", "helloWorld", "value");
		
		Iterable<TestBean> it = JDBCUtils.query(TestBean.class, conn, "select * from test_bean");
		int i = 0;
		for (TestBean bean : it) {
			Assert.assertEquals(bean.name, names[i]);
			i++;
		}
	}
}
