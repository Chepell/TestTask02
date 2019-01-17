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
	private static final String TMP_TABLE = TABLE + "_tmp";
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
	 * метод для получения из БД отфильтрованных данных в виде дерева и вывод на консоль
	 *
	 * @param str строка которая должна быть в именах конечных файлов
	 */
	public void showFilterTree(String str) {
		createTmpTable();
		addToTmpTable(str);
		showFilterTree();
		deleteTmpTable();
	}

	/**
	 * сервисный метод для создания временной таблицы в БД если она еще не создана
	 */
	private void createTmpTable() {
		String query = String.format("CREATE TABLE IF NOT EXISTS %s (\n" +
				"id INT(11) NOT NULL AUTO_INCREMENT,\n" +
				"parent_id INT(11),\n" +
				"name VARCHAR(200) NOT NULL,\n" +
				"PRIMARY KEY (id))\n" +
				"ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=latin1;", TMP_TABLE);
		tableHandler(query);
	}

	/**
	 * сервисный метод отчищения временной таблицы от данных
	 */
	private void clearTmpTable() {
		String query = String.format("TRUNCATE TABLE %s;", TMP_TABLE);
		tableHandler(query);
	}

	/**
	 * сервисный метод удаления временной таблицы из БД
	 */
	private void deleteTmpTable() {
		String query = String.format("DROP TABLE IF EXISTS %s;", TMP_TABLE);
		tableHandler(query);
	}

	/**
	 * сервисный метод для который помещает во врменную таблицу отфильтрованные данные
	 *
	 * @param str подстрока, которая должна содержаться в конечных элементах дерева
	 */
	private void addToTmpTable(String str) {
		String query = String.format("INSERT INTO %s\n" +
				"WITH RECURSIVE cte AS (\n" +
				"SELECT f1.id, f1.parent_id, f1.name AS leaf_list FROM folders f1\n" +
				"LEFT JOIN folders f2 ON f2.parent_id = f1.id\n" +
				"WHERE f2.id IS NULL AND f1.name LIKE '%%%s%%'\n" +
				"UNION ALL\n" +
				"SELECT f.id, f.parent_id, f.name FROM cte\n" +
				"JOIN folders AS f ON cte.parent_id = f.id)\n" +
				"SELECT DISTINCT * FROM cte\n" +
				"ORDER BY id;", TMP_TABLE, str);
		tableHandler(query);
	}

	/**
	 * сервисный метод для вывода на консоль в виде дерева данных из временной таблицы
	 */
	private void showFilterTree() {
		String query = String.format("WITH RECURSIVE cte AS(\n" +
				"SELECT id, parent_id, name, 0 lvl FROM %1$s\n" +
				"WHERE parent_id IS NULL\n" +
				"UNION ALL\n" +
				"SELECT f.id, f.parent_id, CONCAT(REPEAT('  ',cte.lvl + 1), f.name), cte.lvl + 1 FROM cte\n" +
				"JOIN %1$s AS f ON cte.id = f.parent_id)\n" +
				"SELECT name FROM cte\n" +
				"WHERE NOT name = 'ROOT'\n" +
				"ORDER BY id, lvl;", TMP_TABLE);
		queryHandler(query);
	}


	/**
	 * сервисный метод обрабатывающий запрос к БД, вывод на консоль и автоматическое закрытие запроса
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

	private void tableHandler(String query) {
		// создаю обращение к бд в try-with-resources
		try (Statement st = getConnectionToBD().createStatement()) {
			// выполняю запрос и получаю множество результатов
			st.execute(query);
		} catch (SQLException e) {
			System.out.println("Проблема с созданием БД");
		}
	}
}
