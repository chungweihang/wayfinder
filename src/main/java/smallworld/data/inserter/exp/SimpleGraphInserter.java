package smallworld.data.inserter.exp;

import java.io.IOException;

public class SimpleGraphInserter {

	public SimpleGraphInserter(GraphInserter inserter) throws IOException {
		for (int i = 1; i <= 7; i++) {
			inserter.addPerson(i);
		}
		
		inserter.addFriend(1, 2);
		inserter.addFriend(1, 4);
		inserter.addFriend(2, 3);
		inserter.addFriend(2, 4);
		inserter.addFriend(3, 4);
		
		inserter.addFriend(4, 5);
		inserter.addFriend(3, 5);
		
		inserter.addFriend(5, 6);
		inserter.addFriend(5, 7);
		inserter.addFriend(6, 7);
		
		// circle1
		inserter.addCircle("circle1");
		inserter.setCircle("circle1", 1);
		inserter.setCircle("circle1", 2);
		inserter.setCircle("circle1", 3);
		inserter.setCircle("circle1", 4);
		
		// circle2
		inserter.addCircle("circle2");
		inserter.setCircle("circle2", 4);
		inserter.setCircle("circle2", 5);
		
		// circle3
		inserter.addCircle("circle3");
		inserter.setCircle("circle3", 5);
		inserter.setCircle("circle3", 6);
		inserter.setCircle("circle3", 7);
		
		inserter.insert();
	}
}
