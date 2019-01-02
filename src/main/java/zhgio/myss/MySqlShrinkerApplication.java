package zhgio.myss;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import lombok.extern.slf4j.Slf4j;

@SpringBootApplication
@Slf4j
public class MySqlShrinkerApplication {

	public static final String DESTINATION_SCHEMA_URL = "jdbc:mysql://localhost:3306/";
	public static final String DESTINATION_SCHEMA_NAME = "ina_trimmed";
	public static final String DESTINATION_USERNAME = "root";
	public static final String DESTINATION_PASSWORD = "root";

	public static final String ORIGIN_SCHEMA_URL = "jdbc:mysql://localhost:3355/";
	public static final String ORIGIN_SCHEMA_NAME = "?";
	public static final String ORIGIN_USERNAME = "?";
	public static final String ORIGIN_PASSWORD = "?";

	public static final String DB_ENGINE = "InnoDB";
	public static final String DEFAULT_CHARSET = "latin1";
	public static final String DB_DRIVER = "com.mysql.jdbc.Driver";

	public static void main(String[] args) {
		ConfigurableApplicationContext run = SpringApplication.run(MySqlShrinkerApplication.class, args);
		run.close();
	}
}
