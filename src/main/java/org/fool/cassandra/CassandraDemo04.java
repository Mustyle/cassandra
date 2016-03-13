package org.fool.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class CassandraDemo04 {
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
		PreparedStatement statement = session.prepare("insert into testkeyspace.student(name, age) values(?, ?)");
		session.execute(statement.bind("wangwu", 18));

		// select
		session.execute(session.prepare("select * from testkeyspace.student").bind()).all()
				.forEach(row -> System.out.println(row.getString("name") + "\n" + row.getInt("age")));

		// update
		statement = session.prepare("update testkeyspace.student set age = ? where name = ?");
		session.execute(statement.bind(30, "wangwu"));
		session.execute(session.prepare("select * from testkeyspace.student").bind()).all()
				.forEach(row -> System.out.println(row.getString("name") + "\n" + row.getInt("age")));

		// delete
		statement = session.prepare("delete from testkeyspace.student where name = ?");
		session.execute(statement.bind("wangwu"));
		session.execute(session.prepare("select * from testkeyspace.student").bind()).all()
				.forEach(row -> System.out.println(row.getString("name") + "\n" + row.getInt("age")));

		// close session
		session.close();

		// close cluster
		cluster.close();
	}
}
