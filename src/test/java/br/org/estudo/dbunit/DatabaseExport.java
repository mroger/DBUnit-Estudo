package br.org.estudo.dbunit;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.database.search.TablesDependencyHelper;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSet;

public class DatabaseExport {

	public static void main(String[] args) {
		try {
			Class driverClass = Class.forName("org.gjt.mm.mysql.Driver");

			Connection jdbcConnection = DriverManager.getConnection("jdbc:mysql://localhost/sakila", "sakila", "123");
			IDatabaseConnection connection = new DatabaseConnection(jdbcConnection, "sakila");
			connection.getConfig().setProperty(DatabaseConfig.FEATURE_QUALIFIED_TABLE_NAMES,Boolean.TRUE);

			QueryDataSet partialDataSet = new QueryDataSet(connection);
			partialDataSet.addTable("ACTOR", "SELECT * FROM ACTOR LIMIT 0,10");
			partialDataSet.addTable("ADDRESS");
			partialDataSet.addTable("CATEGORY");
			//TODO Find a better way to express this folder
			FlatXmlDataSet.write(partialDataSet, new FileOutputStream("C:/Documents and Settings/Roger/workspace-galileo/dbunit-estudo/src/test/resources/dataset/saida/partial.xml"));

			IDataSet fullDataSet = connection.createDataSet();
			//Careful in uncommenting this line below. The database is somewhat big. 
			//FlatXmlDataSet.write(fullDataSet, new FileOutputStream("C:/Documents and Settings/Roger/workspace-galileo/dbunit-estudo/src/test/resources/dataset/saida/full.xml"));

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
}
