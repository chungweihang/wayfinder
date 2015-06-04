package smallworld.data.inserter.exp;

import java.io.IOException;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;

public interface GraphInserter {

	public static final Label PERSON_LABEL = DynamicLabel.label("Person");
	public static final Label CIRCLE_LABEL = DynamicLabel.label("Circle");
	
	/**
	 * Check if a person is a friend of another.
	 * 
	 * @param from
	 * @param to
	 * @return
	 */
	public boolean isFriend(Object from, Object to);

	// check if a person exists
	public boolean personExists(Object person);

	// check if a circle exists
	public boolean circleExists(String circleName);

	// check if a person belongs to a circle
	// if person and/or circle do not exist, return false
	public boolean hasCirlce(Object person, String circleName);

	// create a circle
	public void addCircle(String circleName);

	// create a circle with features
	// the created circle has label: Circle
	// the features contain name of the circle, i.e., IDENTIFIER : circleName
	public void addCircle(String circleName,
			Map<String, Object> features);

	// add a person to a circle
	// throw exception if either circle or person does not exist
	public void setCircle(String circleName, Object person);

	// create a person
	public void addPerson(Object person);

	// create a person with features
	// the created node has label: Person
	// the features contain name of the circle, i.e., IDENTIFIER : person
	public void addPerson(Object person, Map<String, Object> features);

	/*
	 * Make a person a friend of another. It does nothing if one or both of
	 * people do not exist.
	 */
	public void addFriend(Object fromNode, Object toNode);
	
	public void insert() throws IOException;

}