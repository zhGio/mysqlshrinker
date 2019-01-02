package zhgio.myss.contracts;

import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import zhgio.myss.elements.Column;
import zhgio.myss.elements.Index;
import zhgio.myss.elements.Key;
import zhgio.myss.elements.Table;

public interface MetaDataExplorer {

	long getTableRowLengthApprox(Table table);

	Set<Column> getTableColumnsFromMetaData(Table table) throws SQLException;

	BigDecimal getTableSize(Table table);

	List<Map<String, Object>> getTableDetailsAndExtras(Table table);

	Set<Key> getTablePrimaryKeysFromMetaData(Table table) throws SQLException;

	Set<Key> getTableForeignKeysFromMetaData(Table table);

	Set<Index> getTableIndicesFromMetadata(Table table) throws SQLException;

}
