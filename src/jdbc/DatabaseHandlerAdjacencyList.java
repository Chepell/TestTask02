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
	private static final String VIEW = "view_" + TABLE;
	private static final String ID = "id";
	private static final String PARENT_ID = "parent_id";
	private static final String NAME = "name";
	//
	private Connection dbConnection;

	/**
	 * метод создания коннекшена к БД
	 *
	 * @return возвращает объект коннекшена
	 */
	private synchronized Connection getConnectionToBD() {
		// собираю строку коннешена к БД
		String connectionStr = String.
				format("jdbc:mysql://%s:%s/%s?useSLL=false&serverTimezone=UTC", dbHost, dbPort, dbSchemaName);
		if (dbConnection == null) {
			try {
				dbConnection = DriverManager.getConnection(connectionStr, dbUser, dbPass);
			} catch (SQLException e) {
				System.out.println("Ошибка соединения с БД");
			}
		}
		return dbConnection;
	}

	/**
	 * метод закрытия текущего коннекшена к БД
	 */
	public synchronized void closeConnectionToBD() {
		if (dbConnection != null) {
			try {
				dbConnection.close();
			} catch (SQLException e) {
				System.out.println("Проблема с закрытием коннекшена к БД");
			}
		}
	}

	/**
	 * метод для получения всех данных из БД в виде дерева и вывод на консоль
	 */
	public void showFullTree() {
		String query = String.format("WITH RECURSIVE cte AS(\n" +
				"SELECT id, parent_id, name, 0 lvl FROM %1$s\n" +
				"WHERE parent_id IS NULL\n" +
				"UNION ALL\n" +
				"SELECT f.id, f.parent_id, CONCAT(REPEAT('  ',cte.lvl + 1), f.name), cte.lvl + 1 FROM cte\n" +
				"JOIN %1$s AS f ON cte.id = f.parent_id)\n" +
				"SELECT name FROM cte\n" +
				"WHERE NOT name = 'ROOT'\n" +
				"ORDER BY id, lvl", TABLE);
		queryHandler(query);
	}

	/**
	 * метод для получения из БД отфильтрованных данных
	 * в виде дерева и вывод на консоль через VIEW в запросе
	 *
	 * @param str строка которая должна быть в именах конечных файлов
	 */
	public void showFilterTreeWithView(String str) {
		addFilterTableToView(str);
		showFilterView();
	}

	/**
	 * сервисный метод для который помещает в представление отфильтрованные данные
	 *
	 * @param str подстрока, которая должна содержаться в конечных элементах дерева
	 */
	private void addFilterTableToView(String str) {
		String query = String.format("CREATE OR REPLACE VIEW %s AS\n" +
				"WITH RECURSIVE cte AS (\n" +
				"SELECT f1.id, f1.parent_id, f1.name FROM %s f1\n" +
				"LEFT JOIN %2$s f2 ON f2.parent_id = f1.id\n" +
				"WHERE f2.id IS NULL AND f1.name LIKE '%%%s%%' UNION ALL\n" +
				"SELECT f.id, f.parent_id, f.name FROM cte\n" +
				"JOIN %2$s AS f ON cte.parent_id = f.id)\n" +
				"SELECT DISTINCT * FROM cte ORDER BY id;", VIEW, TABLE, str);
		tableHandler(query);
	}

	/**
	 * сервисный метод для вывода на консоль в виде дерева данных из VIEW
	 */
	private void showFilterView() {
		String query = String.format("WITH RECURSIVE cte AS(\n" +
				"SELECT id, parent_id, name, 0 lvl FROM %s\n" +
				"WHERE parent_id IS NULL UNION ALL\n" +
				"SELECT f.id, f.parent_id, CONCAT(REPEAT('  ',cte.lvl + 1), f.name), cte.lvl + 1 FROM cte\n" +
				"JOIN %1$s AS f ON cte.id = f.parent_id)\n" +
				"SELECT name FROM cte WHERE NOT name = 'ROOT' ORDER BY id, lvl;", VIEW);
		queryHandler(query);
	}

	/**
	 * сервисный метод обрабатывающий запросы к БД без получения данных
	 *
	 * @param query SQL запрос
	 */
	private void tableHandler(String query) {
		// создаю обращение к бд в try-with-resources
		try (Statement st = getConnectionToBD().createStatement()) {
			st.executeUpdate(query);
		} catch (SQLException e) {
			System.out.println("Проблема с созданием TABEL, VIEW");
		}
	}

	/**
	 * сервисный метод обрабатывающий запросы к БД с получением значений
	 * вывод на консоль и автоматическое закрытие запроса
	 *
	 * @param query SQL запрос
	 */
	private void queryHandler(String query) {
		// создаю обращение к бд в try-with-resources
		try (Statement st = getConnectionToBD().createStatement()) {
			// выполняю запрос и получаю множесто результатов
			ResultSet rs = st.executeQuery(query);
			// итерируюсь по множеству
			while (rs.next()) {
				// получаю значение столбца name
				String name = rs.getString(NAME);
				System.out.format("%s\n", name);
			}
		} catch (SQLException e) {
			System.out.println("Проблема с получением данных из БД");
		}
	}
}
