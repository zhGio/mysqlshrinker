package zhgio.myss.runners;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;
import zhgio.myss.MySqlShrinkerApplication;
import zhgio.myss.elements.Column;
import zhgio.myss.elements.Index;
import zhgio.myss.elements.Key;
import zhgio.myss.elements.Table;

import static zhgio.myss.MySqlShrinkerApplication.DB_DRIVER;
import static zhgio.myss.MySqlShrinkerApplication.DESTINATION_PASSWORD;
import static zhgio.myss.MySqlShrinkerApplication.DESTINATION_SCHEMA_NAME;
import static zhgio.myss.MySqlShrinkerApplication.DESTINATION_SCHEMA_URL;
import static zhgio.myss.MySqlShrinkerApplication.DESTINATION_USERNAME;
import static zhgio.myss.MySqlShrinkerApplication.ORIGIN_SCHEMA_NAME;
import static zhgio.myss.MySqlShrinkerApplication.ORIGIN_SCHEMA_URL;

@Configuration
@Slf4j
public class MySSRunner implements CommandLineRunner {

	@Override
	public void run(String... args) {

		log.info("MySQLShrinker application runner starting!");
		try {
			Schemer originSchemer = new Schemer(getDataSourceOrigin());
			Schemer destinationSchemer = new Schemer(getDataSourceDestination());
			List<Table> tables = originSchemer.getAllTablesFromSchema(ORIGIN_SCHEMA_NAME);

			cloneTables(originSchemer, destinationSchemer, tables);
			addConstraints(originSchemer, destinationSchemer, tables);
			setTableSizesAndPrintSorted(originSchemer, tables);

		} catch (SQLException e) {
			log.error("Aborted everything in the Runner#run method.");
		}
	}

	private void setTableSizesAndPrintSorted(Schemer originSchemer, List<Table> tables) {
		tables.forEach(table -> setTableSizes(originSchemer, table));
		tables.sort(Collections.reverseOrder(Comparator.comparingLong(Table::getNumberOfRowsApprox)));
		log.info("SORTING BY ROWS APPROX: ");
		tables.forEach(table -> log.info("table: {} | num of rows approx: {} | size in mb: {}", table.getTableName(), table.getNumberOfRowsApprox(), table.getTableSizeInMb()));

		log.info("SORTING BY MB WEIGHT: ");
		tables.sort(Collections.reverseOrder(Comparator.comparing(table -> table.getTableSizeInMb().intValue())));
		tables.forEach(table -> log.info("table: {} | num of rows approx: {} | size in mb: {}", table.getTableName(), table.getNumberOfRowsApprox(), table.getTableSizeInMb()));

	}

	private void setTableSizes(Schemer originSchemer, Table table) {
		BigDecimal tableSize = originSchemer.getTableSize(table);
		table.setTableSizeInMb(tableSize);
		long tableRowLengthApprox = originSchemer.getTableRowLengthApprox(table);
		table.setNumberOfRowsApprox(tableRowLengthApprox);
	}

	private void addConstraints(Schemer originSchemer, Schemer destinationSchemer, List<Table> tables) {
		tables.forEach(table -> table.setForeignKeys(originSchemer.getTableForeignKeysFromMetaData(table)));
		tables.stream().filter(table -> !table.getForeignKeys().isEmpty()).forEach(Table::writeAlterTableAddFkConstraintsStatement);
		// execute a alter table add fk constraints statement
		tables.stream().filter(table -> !table.getForeignKeys().isEmpty()).forEach(destinationSchemer::executeStatement);
	}

	private void cloneTables(Schemer originSchemer, Schemer destinationSchemer, List<Table> tables) throws SQLException {

		for (Table table : tables) {
			Set<Column> tableColumnsFromMetaData = originSchemer.getTableColumnsFromMetaData(table);
			table.setColumns(tableColumnsFromMetaData);
			List<Map<String, Object>> tableDetailsAndExtras = originSchemer.getTableDetailsAndExtras(table);
			table.setTableDetailsAndExtras(tableDetailsAndExtras);
			Set<Key> tablePrimaryKeysFromMetaData = originSchemer.getTablePrimaryKeysFromMetaData(table);
			table.setPrimaryKeys(tablePrimaryKeysFromMetaData);
			Set<Index> tableIndicesFromMetadata = originSchemer.getTableIndicesFromMetadata(table);
			table.setIndices(tableIndicesFromMetadata);

			table.writeCreateStatement();
		}

		// execute a create table statement
		tables.forEach(destinationSchemer::executeStatement);
	}

	@Bean(name = "dataSourceOrigin")
	public DataSource getDataSourceOrigin() {
		return DataSourceBuilder.create().url(ORIGIN_SCHEMA_URL + ORIGIN_SCHEMA_NAME + "?useSSL=false").username(MySqlShrinkerApplication.ORIGIN_USERNAME)
				.password(MySqlShrinkerApplication.ORIGIN_PASSWORD).driverClassName(DB_DRIVER).build();
	}

	@Bean(name = "dataSourceDestination")
	public DataSource getDataSourceDestination() {
		return DataSourceBuilder.create().url(DESTINATION_SCHEMA_URL + DESTINATION_SCHEMA_NAME + "?useSSL=false").username(DESTINATION_USERNAME).password(DESTINATION_PASSWORD)
				.driverClassName(DB_DRIVER).build();
	}

}
