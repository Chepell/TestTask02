import jdbc.DatabaseHandlerAdjacencyList;

import java.sql.SQLException;

/**
 * Artem Voytenko
 * 15.01.2019
 */

public class Main {
	public static void main(String[] args) throws SQLException, ClassNotFoundException {

		DatabaseHandlerAdjacencyList handlerBase = new DatabaseHandlerAdjacencyList();
		handlerBase.getFullTree();

		System.out.println("\n**************\n");
		handlerBase.getFilterLeafs("d");

		System.out.println("\n**************\n");

		handlerBase.getFilterTree("d");
		System.out.println("\n**************\n");

		handlerBase.closeConnectionToBD();
	}
}
