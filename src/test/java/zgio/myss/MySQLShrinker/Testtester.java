package zgio.myss.MySQLShrinker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

public class Testtester {

	@Test
	public void asdf() {
		List<Integer> asdf = Arrays.asList(1,2,3,4,5,6);
		asdf.stream().filter(i -> i != 3).forEach(System.out::print);
	}
}
