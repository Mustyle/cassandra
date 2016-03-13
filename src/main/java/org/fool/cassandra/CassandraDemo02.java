package org.fool.cassandra;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;

public class CassandraDemo02 {
	public static void main(String[] args) {
		// define a cluster
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

		// get session
		Session session = cluster.connect();

		// create keyspace
		String createKeyspaceSQL = "create keyspace if not exists testkeyspace with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}";
		session.execute(createKeyspaceSQL);

		// create table
		String createTableSQL = "create table if not exists testkeyspace.student(name varchar primary key, age int)";
		session.execute(createTableSQL);

		// insert
		String insertCQL = "insert into testkeyspace.student(name, age) values('zhangsan', 18)";
		session.execute(insertCQL);

		// select
		String queryCQL = "select * from testkeyspace.student";
		ResultSet rs = session.execute(queryCQL);
		List<Row> dataList = rs.all();
		dataList.forEach(row -> System.out.println(row.getString("name") + "\n" + row.getInt("age")));

		// update
		String updateCQL = "update testkeyspace.student set age = 30 where name = 'zhangsan'";
		session.execute(updateCQL);
		session.execute(queryCQL).all().forEach(row -> System.out.println(row.getString("name") + "\n" + row.getInt("age")));

		// delete
		String deleteCQL = "delete from testkeyspace.student where name = 'zhangsan'";
		session.execute(deleteCQL);
		session.execute(queryCQL).all().forEach(row -> System.out.println(row.getString("name") + "\n" + row.getInt("age")));

		// close session
		session.close();

		// close cluster
		cluster.close();
	}
}
