package name.leiqin.jdbc;

import java.util.Date;

public class TestBean {

	String name;
	int age;
	Date brithday;
	@ColumnName("hello_world")
	String helloWorld;
	@ColumnName("data_value")
	double value;

	/**
	 * @return the name
	 */
	public String getName() {
		return name;
	}

	/**
	 * @param name the name to set
	 */
	public void setName(String name) {
		this.name = name;
	}

	/**
	 * @return the age
	 */
	public int getAge() {
		return age;
	}

	/**
	 * @param age the age to set
	 */
	public void setAge(int age) {
		this.age = age;
	}

	/**
	 * @return the brithday
	 */
	public Date getBrithday() {
		return brithday;
	}

	/**
	 * @param brithday the brithday to set
	 */
	public void setBrithday(Date brithday) {
		this.brithday = brithday;
	}

	/**
	 * @return the helloWorld
	 */
	public String getHelloWorld() {
		return helloWorld;
	}

	/**
	 * @param helloWorld the helloWorld to set
	 */
	public void setHelloWorld(String helloWorld) {
		this.helloWorld = helloWorld;
	}

	/**
	 * @return the value
	 */
	public double getValue() {
		return value;
	}

	/**
	 * @param value the value to set
	 */
	public void setValue(double value) {
		this.value = value;
	}

}
