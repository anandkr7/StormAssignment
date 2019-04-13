package com.upgrad.assignment.mysql.services;

import java.sql.PreparedStatement;

import backtype.storm.tuple.Tuple;

public class DatabaseService {

	private MySQLConnection conn;

	/*
	 * CREATE TABLE `storm_assignment`.`word_counts` ( `word` VARCHAR(1024) NOT
	 * NULL, `count` BIGINT(20) NULL, PRIMARY KEY (`word`));
	 */

	public DatabaseService(String ip, String database, String username, String password) {
		conn = new MySQLConnection(ip, database, username, password);
		conn.open();
	}

	public void persist(String word, Long count) {
		PreparedStatement statement = null;

		try {
			statement = conn.getConnection().prepareStatement("insert into word_counts (word, count) values (?, ?)");
			statement.setString(1, word);
			statement.setLong(2, count);

			statement.executeUpdate();
		} catch (Exception ex) {
			ex.printStackTrace();
		} finally {
			if (statement != null) {
				try {
					statement.close();
				} catch (Exception ex) {
					ex.printStackTrace();
				}
			}
		}
	}

	public void close() {
		conn.close();
	}

}
