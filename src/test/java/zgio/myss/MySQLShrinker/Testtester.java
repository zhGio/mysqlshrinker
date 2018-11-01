package zgio.myss.MySQLShrinker;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

public class Testtester {
	List<String> test = Arrays.asList("", "", "a", "b", "", "c");
	@Test
	public void testest() {
		test.stream().filter(str -> !str.isEmpty()).forEach(System.out::println);
	}

}
