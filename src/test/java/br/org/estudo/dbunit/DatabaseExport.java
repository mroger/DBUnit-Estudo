package br.org.estudo.dbunit;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;
import org.dbunit.ext.mysql.MySqlConnection;

public class DatabaseExport {

	/**
	 * This class can be used to extract data from a database to a XML file
	 * @param args
	 * 	-f 				Full database extraction (optional)
	 *  -d <folder> 	Dataset output folder (mandatory)
	 * @throws ParseException 
	 */
	public static void main(String[] args) throws ParseException {
		CommandLine cmd = getCommandLineParser(args);
		if (!cmd.hasOption("d")) {
			throw new IllegalArgumentException("Output folder missing.");
		}
		
		try {
			Class driverClass = Class.forName("org.gjt.mm.mysql.Driver");

			Connection jdbcConnection = DriverManager.getConnection("jdbc:mysql://localhost/sakila", "sakila", "123");
			
			//Foi necessario o uso de uma classe especifica de conexao para resolver o problema da coluna nao ser encontrada
			//http://www.dbunit.org/faq.html#NoSuchColumnException
			IDatabaseConnection connection = new MySqlConnection(jdbcConnection, "sakila");
			connection.getConfig().setProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES, Boolean.TRUE);

			QueryDataSet partialDataSet = new QueryDataSet(connection);
			partialDataSet.addTable("ACTOR", "SELECT * FROM ACTOR LIMIT 0,10");
			partialDataSet.addTable("ADDRESS");
			partialDataSet.addTable("CATEGORY");
			FlatXmlDataSet.write(partialDataSet, new FileOutputStream(cmd.getOptionValue("d") + "/partial.xml"));

			if (cmd.hasOption("-f")) {
				IDataSet fullDataSet = connection.createDataSet();
				FlatXmlDataSet.write(fullDataSet, new FileOutputStream(cmd.getOptionValue("d") + "/full.xml"));
			}

			// dependent tables database export: export table X and all tables
			// that
			// have a PK which is a FK on X, in the right order for insertion
			//TODO Still not working
//			String[] depTableNames = TablesDependencyHelper.getAllDependentTables(connection, "SAKILA.FILM_CATEGORY");
//			IDataSet depDataSet = connection.createDataSet(depTableNames);
//			FlatXmlDataSet.write(depDataSet, new FileOutputStream("C:/Documents and Settings/Roger/workspace-galileo/dbunit-estudo/src/test/resources/dataset/saida/dependents.xml"));
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (DatabaseUnitException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static CommandLine getCommandLineParser(String[] args) throws ParseException {
		Options options = new Options();
		options.addOption("f", false, "extract full database");
		options.addOption("d", true, "output folder");
		CommandLineParser parser = new PosixParser();
		return parser.parse( options, args);
	}

}
