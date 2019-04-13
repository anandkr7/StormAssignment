package com.storm.assignment.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

public class DatabaseService {

	private MySQLConnection conn;

	/*
	 * CREATE TABLE `storm_assignment`.`word_counts` ( `word` VARCHAR(1024) NOT
	 * NULL, `count` BIGINT(20) NULL, PRIMARY KEY (`word`));
	 */

	public DatabaseService(String ip, String database, String username, String password) {
		System.out.println("**************** Initializing DatabaseService ***********************");
		conn = new MySQLConnection(ip, database, username, password);
		conn.open();
	}

	public void persist(String word, Long count) {
		PreparedStatement statement = null;
		Statement stmt = null;
		Map<String, Long> currentStoredData = new HashMap<String, Long>();

		try {

			String sql = "SELECT * FROM word_counts";
			stmt = conn.getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(sql);

			while (rs.next()) {
				Long countRs = rs.getLong("count");
				String wordRs = rs.getString("word");
				currentStoredData.put(wordRs, countRs);
			}

			statement = conn.getConnection().prepareStatement("replace into word_counts (word, count) values (?, ?)");
			statement.setString(1, word);

			if (currentStoredData != null && currentStoredData.get(word) != null) {
				statement.setLong(2, count + currentStoredData.get(word));
			} else {
				statement.setLong(2, count);
			}

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
