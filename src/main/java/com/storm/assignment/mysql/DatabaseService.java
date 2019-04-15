package com.storm.assignment.mysql;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

/**
 * @author Anand DatabaseService code which acts as a service for database
 *         connection and persist and update purposes
 */
public class DatabaseService {

	private MySQLConnection conn;

	public DatabaseService(String ip, String database, String username, String password) {
		System.out.println("**************** Initializing DatabaseService ***********************");
		conn = new MySQLConnection(ip, database, username, password);
		conn.open();
		String DeleteQuery = "DELETE FROM word_counts";
		try {
			Statement stmt;
			stmt = conn.getConnection().createStatement();
			System.out.println("Database clearing");
			stmt.executeUpdate(DeleteQuery);
			System.out.println("Database cleared");
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
	}

	// Persist the word count by checking the current count in the database and
	// update the count
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
