package com.storm.assignment.mysql;

import java.sql.Connection;
import java.sql.DriverManager;

/**
 * @author Anand MySQLConnection code which acts as a MySQL Database connection
 *         provider
 */
public class MySQLConnection {
	private String ip;
	private String database;
	private String username;
	private String password;
	private Connection conn;

	public MySQLConnection(String ip, String database, String username, String password) {
		this.ip = ip;
		this.database = database;
		this.username = username;
		this.password = password;
	}

	public Connection getConnection() {
		return conn;
	}

	public boolean open() {
		boolean successful = true;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection(
					"jdbc:mysql://" + ip + "/" + database + "?" + "user=" + username + "&password=" + password);
		} catch (Exception ex) {
			successful = false;
			ex.printStackTrace();
		}
		return successful;
	}

	public boolean close() {
		if (conn == null) {
			return false;
		}

		boolean successful = true;
		try {
			conn.close();
		} catch (Exception ex) {
			successful = false;
			ex.printStackTrace();
		}

		return successful;
	}
}
