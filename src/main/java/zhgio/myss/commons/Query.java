package zhgio.myss.commons;

public class Query {

	private Query() {}

	// queries
	public static final String QUERY_SIZES_ALL_TABLES = "SELECT table_name AS `Table`, round(((data_length + index_length) / 1024 / 1024), 2) `Size in MB` FROM information_schema.TABLES WHERE table_schema = ? ORDER BY 2 DESC;";
	public static final String QUERY_SIZE_ONE_TABLE = "SELECT round(((data_length + index_length) / 1024 / 1024), 2) AS `size-MB` FROM information_schema.TABLES WHERE table_schema = :tableSchema AND table_name = :tableName;";
	public static final String QUERY_TABLE_STATUS = "SHOW TABLE STATUS WHERE name = :name;";
	public static final String QUERY_TABLE_EXACT_COUNT = "SELECT COUNT(*) FROM ?;";

}
