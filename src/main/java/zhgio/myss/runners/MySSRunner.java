package zhgio.myss.runners;

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import lombok.extern.slf4j.Slf4j;
import zhgio.myss.MySqlShrinkerApplication;
import zhgio.myss.elements.Table;

import static zhgio.myss.commons.Keyword.DESTINATION_SCHEMA_NAME;
import static zhgio.myss.commons.Keyword.DESTINATION_SCHEMA_URL;
import static zhgio.myss.commons.Keyword.ORIGIN_SCHEMA_NAME;
import static zhgio.myss.commons.Keyword.ORIGIN_SCHEMA_URL;

@Configuration
@ConditionalOnClass(DataSource.class)
@Slf4j
public class MySSRunner implements CommandLineRunner {

	@Override
	public void run(String... args) {

		log.info("MySQLShrinker application runner starting!");
		try {
			Schemer originSchemer = new Schemer(getDataSourceOrigin());
			Schemer destinationSchemer = new Schemer(getDataSourceDestination());
			List<Table> tables = originSchemer.getAllTablesFromSchema(ORIGIN_SCHEMA_NAME);

			for (Table table : tables) {
				table.setTableColumns();
				table.setTableDetailsAndExtras();
				table.setTableSize();
				table.setTableRowLengthApprox();
				table.setTablePrimaryKeys();
				table.setTableIndices();
				table.writeCreateStatement();
			}

			tables.forEach(destinationSchemer::executeStatement);

			tables.forEach(Table::setTableForeignKeys);
			tables.stream().filter(table -> !table.getForeignKeys().isEmpty()).forEach(Table::writeAlterTableAddFkConstraintsStatement);
			tables.stream().filter(table -> !table.getForeignKeys().isEmpty()).forEach(destinationSchemer::executeStatement);

		} catch (SQLException e) {
			log.error("Aborted everything in the Runner#run method.");
		}
	}

	@Bean(name = "dataSourceOrigin")
	public DataSource getDataSourceOrigin() {
		return DataSourceBuilder.create().url(ORIGIN_SCHEMA_URL + ORIGIN_SCHEMA_NAME).username(MySqlShrinkerApplication.USER_NAME).password(MySqlShrinkerApplication.PASSWORD)
				.driverClassName("com.mysql.jdbc.Driver").build();
	}

	@Bean(name = "dataSourceDestination")
	public DataSource getDataSourceDestination() {
		return DataSourceBuilder.create().url(DESTINATION_SCHEMA_URL + DESTINATION_SCHEMA_NAME).username("root").password("root").driverClassName("com.mysql.jdbc.Driver").build();
	}

}
