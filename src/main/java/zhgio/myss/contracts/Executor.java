package zhgio.myss.contracts;

import java.util.List;

import zhgio.myss.elements.Table;

public interface Executor {

	void executeStatement(Table table);

	List<Table> getAllTablesFromSchema(String schemaName);

}
