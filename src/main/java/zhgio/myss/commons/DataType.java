package zhgio.myss.commons;

public enum DataType {
	// Regular MySQL Data Types
	CHAR, VARCHAR, BLOB, ENUM, // text data types
	TINYINT, SMALLINT, INT, BIGINT, FLOAT, DOUBLE, DECIMAL, // numeric data types
	DATE, TIME, TIMESTAMP, DATETIME,// date data types

	// added from java.sql.Types
	BOOLEAN, BIT

}