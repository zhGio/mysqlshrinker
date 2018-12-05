package zhgio.myss.elements;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import zhgio.myss.commons.DataType;

import static zhgio.myss.commons.Keyword.EMPTY_STR;

@Data
@AllArgsConstructor
@RequiredArgsConstructor
@Slf4j
public class Column {

	private static final int DECIMAL_DIGITS_DEFAULT_VALUE = 0;

	@ToString.Exclude
	private final Table table;
	private String columnName;
	private DataType type;
	private int columnSize;
	@ToString.Exclude
	private boolean nullable;
	@ToString.Exclude
	private boolean autoincrement;
	@ToString.Exclude
	private boolean defaultable;
	@ToString.Exclude
	private String defaultValue;
	private int decimalDigits;
	@ToString.Exclude
	private boolean isUnsigned;
	@ToString.Exclude
	private List<String> enums;
	@ToString.Exclude
	private String extra;

	public String getDefaultValue() { // datetime types throw an sql grammar exception if create table statement is executed with a month 00 and day 00
		if (this.type == DataType.DATE || this.type == DataType.DATETIME || this.type == DataType.TIMESTAMP || this.type == DataType.TIME) {
			if (StringUtils.contains(this.defaultValue, "-00-00")) {
				return StringUtils.replace(this.defaultValue, "-00-00", "-01-01");
			}
		}
		return defaultValue;
	}

	/**
	 * extract info like columnSize, decimalDigits precision and signed/unsigned from the Type column produced by DESC `tablename`
	 * @param type - a string type which will equal to something like "int(11)" or "decimal(12,6)" so we want to extract data between parentheses only
	 */
	public void updateColumnFromTypeString(Object type) {
		String typeStr = (String) type;
		Matcher matcher = Pattern.compile("\\((.*?)\\)").matcher(typeStr);
		log.debug("matching type {} for column {} in table {}", typeStr, this.columnName, this.table.getTableName());
		if (matcher.find()) {
			String matchGroup = matcher.group(1);
			if (StringUtils.startsWith(typeStr, "decimal")) {
				parseDecimalTypeValues(matchGroup);
			} else if (StringUtils.startsWith(typeStr, "enum")) {
				// enum type is wrongly set as type CHAR so we change it here to our own enum
				this.setType(DataType.ENUM);
				parseEnumTypeValues(matchGroup);
			} else {
				log.debug("setting default type values for string {}", matchGroup);
				this.columnSize = Integer.valueOf(matchGroup);
				this.decimalDigits = DECIMAL_DIGITS_DEFAULT_VALUE;
			}
		}
		this.isUnsigned = StringUtils.contains(typeStr, "unsigned");
	}

	/**
	 * Extract info from the "Extra" column produced by DESC `tablename`
	 * @param extra - a string with Extra information about the column like auto_increment, on update rules, etc
	 */
	public void updateColumnFromExtraString(Object extra) {
		log.debug("parsing `Extra` column from DESC");
		String extraStr = (String) extra;
		if (!"auto_increment".equals(extraStr)) { // auto_increment is retrieved in a different way
			this.extra = extraStr;
		}
	}

	private void parseEnumTypeValues(String matchGroup) {
		log.debug("parsing enum type values for string {}", matchGroup);
		if (matchGroup.contains(",")) {
			this.enums = Arrays.stream(matchGroup.split(",")).map(String::trim).collect(Collectors.toList());
		} else {
			this.enums = Collections.singletonList(matchGroup);
		}
	}

	private void parseDecimalTypeValues(String matchGroup) {
		log.debug("parsing decimal type values for string {}", matchGroup);
		if (matchGroup.contains(",")) {
			int[] numbers = Arrays.stream(matchGroup.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
			this.columnSize = numbers[0];
			this.decimalDigits = numbers[1];
		} else {
			this.columnSize = Integer.valueOf(matchGroup);
			this.decimalDigits = DECIMAL_DIGITS_DEFAULT_VALUE;
		}
	}

	public boolean isAnyDateTimeAndCurrentTimestamp(String currentDefault) {
		return (this.getType() == DataType.DATE || this.getType() == DataType.TIME || this.getType() == DataType.TIMESTAMP) && StringUtils.equalsIgnoreCase("CURRENT_TIMESTAMP", currentDefault);
	}

	public boolean isBitOrBoolean() {
		return this.getType().equals(DataType.BIT) || this.getType().equals(DataType.BOOLEAN);
	}

	public String appendColumnDetails() {
		int columnSize = this.getColumnSize();
		if (DataType.DECIMAL == this.getType()) {
			int decimalDigits = this.getDecimalDigits();
			return "(" + (columnSize == 0 ? EMPTY_STR : columnSize) + "," + (decimalDigits == 0 ? EMPTY_STR : decimalDigits) + ")";
		} else if (DataType.ENUM == this.getType() && !this.getEnums().isEmpty()) {
			return "(" + String.join(",", this.getEnums()) + ")";
		} else if (DataType.TIMESTAMP != this.getType() && DataType.DATE != this.getType() && DataType.TIME != this.getType()) { // dates have no precision
			return (columnSize == 0 ? EMPTY_STR : "(" + columnSize + ")");
		} else {
			return EMPTY_STR;
		}
	}

}
