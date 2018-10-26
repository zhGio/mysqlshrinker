package zhgio.myss;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import lombok.extern.log4j.Log4j2;

@SpringBootApplication
@Log4j2
public class MySqlShrinkerApplication {

	static final String USER_NAME = "?";
	static final String PASSWORD = "?";

	public static void main(String[] args) {
		ConfigurableApplicationContext run = SpringApplication.run(MySqlShrinkerApplication.class, args);
		run.close();
	}
}
