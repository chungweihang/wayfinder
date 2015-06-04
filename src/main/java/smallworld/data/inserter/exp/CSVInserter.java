package smallworld.data.inserter.exp;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import smallworld.data.RelationshipTypes;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

public class CSVInserter implements GraphInserter {

	final String path;
	final boolean isFriendshipDirected;
	Map<Object, Node> people;
	Multimap<Object, Object> friendEdges;
	Multimap<String, Object> circleEdges;
	Map<String, Node> circles;

	public CSVInserter(String path, boolean isFriendshipDirected) {
		this.path = path;
		this.isFriendshipDirected = isFriendshipDirected;
		people = new HashMap<>();
		circles = new HashMap<>();
		friendEdges = HashMultimap.create();
		circleEdges = HashMultimap.create();
	}
	
	@Override
	public boolean isFriend(Object from, Object to) {
		if (isFriendshipDirected) {
			// directed
			return friendEdges.containsEntry(from, to);
		} else {
			// undirected
			return friendEdges.containsEntry(from, to)
					|| friendEdges.containsEntry(to, from);
		}
	}

	@Override
	public boolean personExists(Object person) {
		return people.containsKey(person);
	}

	@Override
	public boolean circleExists(String circleName) {
		return circles.containsKey(circleName);
	}

	@Override
	public boolean hasCirlce(Object person, String circleName) {
		return circleEdges.containsEntry(circleName, person);
	}

	@Override
	public void addCircle(String circleName) {
		addCircle(circleName, null);
	}

	@Override
	public void addCircle(String circleName, Map<String, Object> features) {
		if (!circles.containsKey(circleName)) {
			Node node = new Node();
			node.id = circleName;
			node.label = CIRCLE_LABEL.toString();
			node.properties = features;
			circles.put(circleName, node);
		}else {
			// TODO: combine features;
		}
	}

	@Override
	public void setCircle(String circleName, Object person) {
		if (circleExists(circleName) && personExists(person) && !hasCirlce(person, circleName)) {
			circleEdges.put(circleName, person);
		}
	}

	@Override
	public void addPerson(Object person) {
		addPerson(person, null);
	}

	@Override
	public void addPerson(Object person, Map<String, Object> features) {
		if (!people.containsKey(person)) {
			Node node = new Node();
			node.id = person;
			node.label = PERSON_LABEL.toString();
			node.properties = features;
			people.put(person, node);
		} else {
			// TODO: combine features;
		}
	}

	@Override
	public void addFriend(Object from, Object to) {
		if (personExists(from) && personExists(to) && !isFriend(from, to)) {
			friendEdges.put(from, to);
		}
	}

	@Override
	public void insert() throws IOException {
		printNodes(people.values(), "people", path + "/people.csv");
		printNodes(circles.values(), "circle", path + "/cirlces.csv");
		printRelationships(friendEdges, RelationshipTypes.FRIEND.toString(), path + "/friend-edges.csv");
		printRelationships(circleEdges, RelationshipTypes.CIRCLE.toString(), path + "/circles-edges.csv");
	}
	
	private static void printRelationships(Multimap<? extends Object, Object> edges, String type, String filename) throws IOException {
		try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
			writer.println(":START_ID,:END_ID,:TYPE");
			for (Map.Entry<? extends Object, Object> entry : edges.entries()) {
				writer.println("\"" + entry.getValue() + "\",\"" + entry.getKey() + "\"," + type);
			}
		}
	}
	
	private static void printNodes(Collection<Node> nodes, String type, String filename) throws IOException {
		// save node
		LinkedHashSet<String> properties = null; // to fix the order
		try (PrintWriter writer = new PrintWriter(new FileWriter(filename))) {
			for (Node n : nodes) {
				if (properties == null) {
					properties = new LinkedHashSet<>();
					// print header
					writer.print(type + ":ID,");
					if (n.properties != null) {
						properties = new LinkedHashSet<>(n.properties.keySet());
						for (String property : properties) {
							writer.print(property + ",");
						}
					} 
					writer.println(":LABEL");
				}
				writer.print("\"" + n.id + "\",");
				if (n.properties != null) {
					for (String property : properties) {
						writer.print(n.properties.get(property) + ",");
					}
				}
				writer.println(n.label);
			}
		}
	}
	
	
	class Node {
		Object id;
		Map<String, Object> properties;
		String label;
	}
	
	class Relationship {
		String from;
		String to;
		String type;
	}
}
