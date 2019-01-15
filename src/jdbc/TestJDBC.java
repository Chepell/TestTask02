package jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Artem Voytenko
 * 15.01.2019
 */

public class TestJDBC {
	public static void main(String[] args) {
		// линк к нужной БД
		String jdbcUrl = "jdbc:mysql://localhost:3306/sber_tabel?useSLL=false&serverTimezone=UTC";
		String user = "sbertest";
		String password = "sbertest";

		try (Connection connection = DriverManager.getConnection(jdbcUrl, user, password)) {
			System.out.println("Connecting to DB: " + jdbcUrl);
			System.out.println("Connection successful!!!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}
