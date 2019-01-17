package jdbc;

import java.sql.*;


/**
 * Artem Voytenko
 * 15.01.2019
 * Класс обработчик коннекта к БД и запросов
 */

public class DatabaseHandlerAdjacencyList {
	// параметры для коннекта к БД
	private String dbHost = "localhost";
	private String dbPort = "3306";
	private String dbUser = "sbertest";
	private String dbPass = "sbertest";
	private String dbSchemaName = "sber_tabel";
	private static final String TABLE = "folders";
	private static final String ID = "id";
	private static final String PARENT_ID = "parent_id";
	private static final String NAME = "name";
	//
	private Connection dbConnection;

	// метод возвращает коннекшн к БД
	public Connection getDbConnection() throws SQLException {
		// собираю строку коннешена
		String connectionStr = String.
				format("jdbc:mysql://%s:%s/%s?useSLL=false&serverTimezone=UTC", dbHost, dbPort, dbSchemaName);

		// создаю коннешн и сохраняю его в поле объекта класса
		dbConnection = DriverManager.getConnection(connectionStr, dbUser, dbPass);
//		System.out.println("Connection successful!!!");
		return dbConnection;
	}

	/**
	 * метод для получения данных из БД в виде дерева и вывода на консоль
	 * @param str строка которая должна быть в именах
	 */
	public void getFilterTree(String str) {
		// формат запроса на получение данных из БД
		String query = String.format("WITH RECURSIVE cte AS(\n" +
				"SELECT id, parent_id, name, 0 lvl FROM folders\n" +
				"WHERE parent_id IS NULL\n" +
				"UNION ALL\n" +
				"SELECT f.id, f.parent_id, CONCAT(REPEAT('  ',cte.lvl + 1), f.name), cte.lvl + 1 FROM cte\n" +
				"JOIN folders AS f ON cte.id = f.parent_id)\n" +
				"SELECT name FROM cte\n" +
				"WHERE NOT name = 'ROOT' AND name LIKE '%%%s%%' \n" +
				"ORDER BY id, lvl", str);

		// создаю обращение к бд в try-with-resources
		try (Statement st = getDbConnection().createStatement()) {
			// выполняю запрос и получаю множесто результатов
			ResultSet rs = st.executeQuery(query);
			// итерируюсь по множеству
			while (rs.next()) {
				// получаю значение столбца name
				String name = rs.getString(NAME);
				System.out.format("%s\n", name);
			}
		} catch (SQLException e) {
			System.out.println("Проблема с доступом к БД");
		}
	}

	/**
	 * метод для получения всех данных из БД в виде дерева и вывода на консоль
	 */
	public void getFullTree() {
		// вызываю метод для получения фильтрованных данных, но с параметров в виде пустой строки
		getFilterTree("");
	}


	/**
	 * метод закрытия текущего коннекшена к БД
	 */
	public void closeConnectionToBD() {
		if (dbConnection != null) {
			try {
				dbConnection.close();
			} catch (SQLException e) {
				System.out.println("Проблема с закрытием коннекшена к БД");
			}
		}
	}
}
