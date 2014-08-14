## 使用方法

### 建表：

	JDBCUtils.update(conn, "DROP TABLE IF EXISTS test_bean");
	JDBCUtils.update(conn, "CREATE TABLE IF NOT EXISTS test_bean (id integer PRIMARY KEY AUTOINCREMENT, name text, age integer, brithday timestamp, hello_world text, data_value real)");

### 更新：

	INSERT_SQL = "INSERT INTO test_bean (name, age, brithday, hello_world, data_value) VALUES (?, ?, ?, ?, ?)";

	Date brithday = new Date();
	TestBean testBean = createBean("Hello world", 18, "Ha Ha Ha", brithday, 1.23);	
	JDBCUtils.insert(conn, INSERT_SQL,
			JDBCUtils.params(testBean, "name", "age", "brithday", "helloWorld", "value"));

### 批量更新：

	String[] names = {"AAA", "BBB", "CCC", "DDD", "EEE"};
	List<TestBean> list = new ArrayList<>();
	for (String name : names) {
		TestBean tb = createBean(name, 18, "Hello", new Date(), 1.23);
		list.add(tb);
	}
	JDBCUtils.batch(conn, INSERT_SQL, list, "name", "age", "brithday", "helloWorld", "value");

### 查询：

	TestBean queryBean = JDBCUtils.queryOne(TestBean.class, conn, "select * from test_bean");

	long count = JDBCUtils.count(conn, "select count(*) from test_bean");

	Iterable<TestBean> it = JDBCUtils.query(TestBean.class, conn, "select * from test_bean");
	for (TestBean bean : it) {
		// TODO
	}

