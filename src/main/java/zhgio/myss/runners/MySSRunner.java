package zhgio.myss.runners;

import java.math.BigDecimal;
import java.sql.SQLException;
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
	public void run(String... args){

		log.info("MySQLShrinker application runner starting!");
		try {
			Schemer originSchemer = new Schemer(getDataSourceOrigin());
			Schemer destinationSchemer = new Schemer(getDataSourceDestination());
			List<Table> tables = cloneTables(originSchemer, destinationSchemer);
			addConstraints(originSchemer, destinationSchemer, tables);

		} catch (SQLException e) {
			log.error("Aborted everything in the Runner#run method.");
		}
	}

	private void addConstraints(Schemer originSchemer, Schemer destinationSchemer, List<Table> tables) {
		tables.forEach(table -> table.setForeignKeys(originSchemer.getTableForeignKeysFromMetaData(table)));
		tables.stream().filter(table -> !table.getForeignKeys().isEmpty()).forEach(Table::writeAlterTableAddFkConstraintsStatement);
		// execute a alter table add fk constraints statement
		tables.stream().filter(table -> !table.getForeignKeys().isEmpty()).forEach(destinationSchemer::executeStatement);
	}

	private List<Table> cloneTables(Schemer originSchemer, Schemer destinationSchemer) throws SQLException {
		List<Table> tables = originSchemer.getAllTablesFromSchema(ORIGIN_SCHEMA_NAME);

		for (Table table : tables) {
			Set<Column> tableColumnsFromMetaData = originSchemer.getTableColumnsFromMetaData(table);
			table.setColumns(tableColumnsFromMetaData);
			List<Map<String, Object>> tableDetailsAndExtras = originSchemer.getTableDetailsAndExtras(table);
			table.setTableDetailsAndExtras(tableDetailsAndExtras);
			BigDecimal tableSize = originSchemer.getTableSize(table);
			table.setTableSizeInMb(tableSize);
			long tableRowLengthApprox = originSchemer.getTableRowLengthApprox(table);
			table.setNumberOfRowsApprox(tableRowLengthApprox);
			Set<Key> tablePrimaryKeysFromMetaData = originSchemer.getTablePrimaryKeysFromMetaData(table);
			table.setPrimaryKeys(tablePrimaryKeysFromMetaData);
			Set<Index> tableIndicesFromMetadata = originSchemer.getTableIndicesFromMetadata(table);
			table.setIndices(tableIndicesFromMetadata);

			table.writeCreateStatement();
		}

		// execute a create table statement
		tables.forEach(destinationSchemer::executeStatement);
		return tables;
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
