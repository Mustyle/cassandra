package org.fool.cassandra;

import java.util.List;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public class CassandraDemo03 {
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
		Insert insert = QueryBuilder.insertInto("testkeyspace", "student").value("name", "lisi").value("age", 40);
		System.out.println(insert);
		session.execute(insert);
		
		// select
		com.datastax.driver.core.querybuilder.Select.Where select = QueryBuilder.select().all().from("testkeyspace", "student").where(QueryBuilder.eq("name", "lisi"));
		System.out.println(select);
		ResultSet rs = session.execute(select);
		List<Row> dataList = rs.all();
		dataList.forEach(row -> System.out.println(row.getString("name") + "\n" + row.getInt("age")));
		
		// update
		com.datastax.driver.core.querybuilder.Update.Where update = QueryBuilder.update("testkeyspace", "student").with(QueryBuilder.set("age", 30)).where(QueryBuilder.eq("name", "lisi"));
		System.out.println(update);
		session.execute(update);
		session.execute(select).all().forEach(row -> System.out.println(row.getString("name") + "\n" + row.getInt("age")));
		
		// delete
		com.datastax.driver.core.querybuilder.Delete.Where delete = QueryBuilder.delete().from("testkeyspace", "student").where(QueryBuilder.eq("name", "lisi"));
		System.out.println(delete);
		session.execute(delete);
		session.execute(select).all().forEach(row -> System.out.println(row.getString("name") + "\n" + row.getInt("age")));
		
		// close session
		session.close();

		// close cluster
		cluster.close();
	}
}
