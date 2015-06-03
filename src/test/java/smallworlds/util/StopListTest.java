package smallworlds.util;

import org.junit.Assert;
import org.junit.Test;

import smallworld.util.StopList;

public class StopListTest {

	@Test
	public void test() {
		Assert.assertTrue(StopList.INSTANCE.isStopword("an"));
		Assert.assertTrue(StopList.INSTANCE.isStopword("with"));
		Assert.assertFalse(StopList.INSTANCE.isStopword("happy"));
	}
}
