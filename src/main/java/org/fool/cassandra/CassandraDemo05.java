package org.fool.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;

public class CassandraDemo05 {
	public static void main(String[] args) {
		QueryOptions options = new QueryOptions();
		options.setConsistencyLevel(ConsistencyLevel.QUORUM);

		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").withQueryOptions(options).build();

		Session session = cluster.connect();

		String createKeyspaceSQL = "create keyspace if not exists testkeyspace with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 1}";
		session.execute(createKeyspaceSQL);

		String createTableSQL = "create table if not exists testkeyspace.student(name varchar primary key, age int)";
		session.execute(createTableSQL);

		PreparedStatement statement = session.prepare("insert into testkeyspace.student(name, age) values(?, ?)");
		statement.setConsistencyLevel(ConsistencyLevel.ONE);
		session.execute(statement.bind("zhaoliu", 18));

		session.close();
		cluster.close();
	}
}
