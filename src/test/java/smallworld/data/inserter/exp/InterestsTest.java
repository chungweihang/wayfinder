package smallworld.data.inserter.exp;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import edu.stanford.nlp.util.Maps;

public class InterestsTest {

	@Test
	public void test() {
		Map<String, Object> interests = new HashMap<>();
		Interests.addInterests(interests, "hello-world to add and 'subtract' 1 with super8");
		Interests.addInterests(interests, "hello world with \"addition\"!?");
		System.out.println(Maps.toStringSorted(interests));
	}
}
