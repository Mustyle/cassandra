package org.fool.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;

public class CassandraDemo01 {
	public static void main(String[] args) {
		Cluster cluster = Cluster.builder().addContactPoint("127.0.0.1").build();

		Metadata metadata = cluster.getMetadata();
		
		metadata.getAllHosts().forEach(host -> System.out.println(host.getAddress()));

		System.out.println("===============");

		metadata.getKeyspaces().forEach(keyspaceMetadata -> System.out.println(keyspaceMetadata.getName()));
		
		cluster.close();
	}
}
