import jdbc.DatabaseHandlerAdjacencyList;

/**
 * Artem Voytenko
 * 15.01.2019
 */

public class Main {
	public static void main(String[] args) {

		DatabaseHandlerAdjacencyList handlerBase = new DatabaseHandlerAdjacencyList();

		handlerBase.getFullTree();

		System.out.println("\n**************\n");

		handlerBase.getFilterTree("f");


		handlerBase.closeConnectionToBD();
	}
}