package smallworld.data.inserter.exp;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import smallworld.data.RelationshipTypes;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

/**
 * Neo4JInserter inserts a social network graph into Neo4J database. A social
 * network graph contains people, circles, and friends. Peoples can be friends
 * with others, and people can be belongs to none or multiple circles.
 * 
 * By default, friendship is bidirectional. That is, Alice is a friend of Bob
 * implies Bob is a also friend of Alice. Friendship can be changed to
 * directional by using constructor {@code Neo4JInserter(String, true)}.
 * 
 * In Neo4J representation, both people and circles are modeled as nodes. People
 * connect to others via "FRIED" relationships, whereas people connect to
 * circles via "CIRCLE" relationships.
 * 
 * Both people and circles can have features.
 * 
 * @author chang
 *
 */
public class Neo4JInserter {

	private static final Logger logger = LogManager.getLogger();
	private static final String IDENTIFIER = "dataset-identifier";

	final BatchInserter inserter;

	// indicate if friendship is directional or not (bidirectional)
	final boolean isFriendshipDirected;

	// keep track of who is friend of whom
	final Multimap<Long, Long> friendEdges;

	// keep track of who belongs to which circles
	final Multimap<Long, Long> circleEdges;

	// keep track of node IDs of people
	final Map<Object, Long> personToIds;

	// keep track of node IDs of circles
	final Map<String, Long> circleToIds;

	// statistics
	int maxCircle = 0; // need to set explicitly
	long totalCircleSize = 0;

	/**
	 * Initialize Neo4JInserter with the path of Neo4J database.
	 * 
	 * @param path
	 *            the path where the Neo4J database is created
	 */
	public Neo4JInserter(String path) {
		this(path, false);
	}

	/**
	 * Initialize Neo4JInserter with the path of Neo4J database and if the
	 * friendship is directional.
	 * 
	 * @param path
	 *            the path where the Neo4J database is inserted
	 * @param isFriendshipDirected
	 *            if the friendship is directional
	 */
	public Neo4JInserter(String path, boolean isFriendshipDirected) {
		// Neo4J batchinserter config
		Map<String, String> config = new HashMap<>();
		config.put("neostore.nodestore.db.mapped_memory", "90M");
		config.put("neostore.relationshipstore.db.mapped_memory", "3G");
		config.put("neostore.propertystore.db.mapped_memory", "50M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "100M");
		config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");
		inserter = BatchInserters.inserter(path, config);

		this.isFriendshipDirected = isFriendshipDirected;

		friendEdges = HashMultimap.create();
		circleEdges = HashMultimap.create();
		personToIds = new HashMap<>();
		circleToIds = new HashMap<>();
	}

	/**
	 * Insert the created social network graph and print out statistics. It will
	 * delete the existing folder before inserting.
	 * 
	 * @throws IOException
	 */
	void insert() throws IOException {
		delete(inserter.getStoreDir());
		inserter.shutdown();

		logger.info("Number of nodes: " + personToIds.size());
		logger.info("Number of friendships: " + friendEdges.size());
		logger.info("Number of circles: " + circleToIds.size());
		logger.info("Max size of circle: " + maxCircle);
		logger.info("Average circle size: " + ((double) totalCircleSize)
				/ circleToIds.size());
		logger.info("Average membership size: " + ((double) totalCircleSize)
				/ personToIds.size());

	}

	/**
	 * Check if a person is a friend of another.
	 * 
	 * @param from
	 * @param to
	 * @return
	 */
	public boolean isFriend(Object from, Object to) {
		if (personExists(from) && personExists(to))
			return relationshipExists(RelationshipTypes.FRIEND.type(),
					personToIds.get(from), personToIds.get(to));
		return false;
	}

	// check if a person exists
	public boolean personExists(Object person) {
		return personToIds.containsKey(person);
	}

	// check if a circle exists
	public boolean circleExists(String circleName) {
		return circleToIds.containsKey(circleName);
	}

	// check if a person belongs to a circle
	// if person and/or circle do not exist, return false
	public boolean hasCirlce(Object person, String circleName) {
		if (personExists(person) && circleExists(circleName)) {
			return circleEdges.containsEntry(personToIds.get(person),
					circleToIds.get(circleName));
		}

		return false;
	}

	// create a circle
	public void addCircle(String circleName) {
		addCircle(circleName, emptyMap());
	}

	// create a circle with features
	// the features contain name of the circle, i.e., IDENTIFIER : circleName
	public void addCircle(String circleName, Map<String, Object> features) {
		if (!circleExists(circleName)) {
			features.put(IDENTIFIER, circleName); // add circleName as property
												// IDENTIFIER
			long id = inserter.createNode(features);
			circleToIds.put(circleName, id);
		} else {
			long id = circleToIds.get(circleName);
			features.putAll(inserter.getNodeProperties(id));
			inserter.setNodeProperties(id, features);
		}
	}

	// add a person to a circle
	// throw exception if either circle or person does not exist
	public void setCircle(String circleName, Object person) {
		if (!personExists(person)) {
			throw new IllegalArgumentException("Node: " + person
					+ " does not exist!");
		}

		if (circleExists(circleName)) {
			// always from node to circle
			if (addRelationship(personToIds.get(person),
					circleToIds.get(circleName),
					RelationshipTypes.CIRCLE.type())) {
				circleEdges.put(personToIds.get(person),
						circleToIds.get(circleName));
				totalCircleSize++;
			}
		} else {
			throw new IllegalArgumentException("Cirlce: " + circleName
					+ " does not exist!");
		}
	}

	// create a person
	public void addPerson(Object person) {
		addPerson(person, emptyMap());
	}

	// create a person with features
	// the features contain name of the circle, i.e., IDENTIFIER : circleName
	public void addPerson(Object person, Map<String, Object> features) {
		if (!personExists(person)) {
			features.put(IDENTIFIER, person);
			long id = inserter.createNode(features);
			personToIds.put(person, id);
		} else {
			long id = personToIds.get(person);
			features.putAll(inserter.getNodeProperties(id));
			inserter.setNodeProperties(id, features);
		}
	}

	// utility method to create an empty map
	private static Map<String, Object> emptyMap() {
		return new HashMap<>();
	}

	// utility method for adding relationship to neo4j
	// return true if relationship is added
	// return false if it already exists
	private boolean addRelationship(long fromNodeId, long toNodeId,
			RelationshipType type) {
		if (!relationshipExists(type, fromNodeId, toNodeId)) {
			// relationship does not exist, create new relationship
			inserter.createRelationship(fromNodeId, toNodeId, type, null);
			return true;
		} else {
			return false;
		}
	}

	/*
	 * Make a person a friend of another. It does nothing if one or both of
	 * people do not exist.
	 */
	public void addFriend(Object fromNode, Object toNode) {
		if (!personExists(fromNode)) {
			throw new IllegalArgumentException("Node: " + fromNode
					+ " does not exist!");
		}

		if (!personExists(toNode)) {
			throw new IllegalArgumentException("Node: " + toNode
					+ " does not exist!");
		}
		if (addRelationship(personToIds.get(fromNode), personToIds.get(toNode),
				RelationshipTypes.FRIEND.type())) {
			friendEdges.put(personToIds.get(fromNode), personToIds.get(toNode));
		}
	}

	/*
	 * Check if a relationship exists. For friendship, if it is bidirectional,
	 * check both "from => to" and "to => from." If friendship is directional,
	 * check only "from => to."
	 * 
	 * For circle relationship, always check "from (person) => to (circle)."
	 */
	private boolean relationshipExists(RelationshipType type, long fromNodeId,
			long toNodeId) {
		if (type == RelationshipTypes.FRIEND.type()) {
			if (isFriendshipDirected) {
				// directed
				return friendEdges.containsEntry(fromNodeId, toNodeId);
			} else {
				// undirected
				return friendEdges.containsEntry(fromNodeId, toNodeId)
						|| friendEdges.containsEntry(toNodeId, fromNodeId);
			}
		} else if (type == RelationshipTypes.CIRCLE.type()) {
			// for circle, it is always node => circle
			return circleEdges.containsEntry(fromNodeId, toNodeId);
		} else {
			throw new IllegalArgumentException("Unknown relation type: "
					+ type.name());
		}

	}

	// utility method for deleting a folder
	private void delete(String path) throws IOException {
		FileUtils.deleteRecursively(new File(path));
	}
}
