package smallworld.data.inserter.exp;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;
import org.neo4j.unsafe.batchinsert.BatchRelationship;

import smallworld.data.RelationshipTypes;
import smallworld.util.Utils;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

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
 * Both people and circles can have features. People nodes will be labeled as
 * "Person", and circle nodes will be labeled as "Circle."
 * 
 * @author chang
 *
 */
public class Neo4JInserter implements GraphInserter {

	private static final Logger logger = LogManager.getLogger();
	public static final String IDENTIFIER = "DATASET_IDENTIFIER";
	public static long CACHE_MAX_SIZE = 10000;
	
	final BatchInserter inserter;

	// indicate if friendship is directional or not (bidirectional)
	final boolean isFriendshipDirected;

	// keep track of who is friend of whom
	//final Multimap<Long, Long> friendEdges;

	// keep track of who belongs to which circles
	//final Multimap<Long, Long> circleEdges;

	// keep track of node IDs of people
	final Map<Object, Long> personToIds;

	// keep track of node IDs of circles
	final Map<String, Long> circleToIds;
	
	final LoadingCache<Long, Set<Long>> friends;

	// statistics
	int maxCircle = 0; // need to set explicitly
	long totalCircleSize = 0;
	boolean enforceUniqueRelationships = true;

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
		
		try {
			delete(path);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Neo4J batchinserter config
		Map<String, String> config = new HashMap<>();
		config.put("neostore.nodestore.db.mapped_memory", "90M");
		config.put("neostore.relationshipstore.db.mapped_memory", "3G");
		config.put("neostore.propertystore.db.mapped_memory", "50M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "100M");
		config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");
		inserter = BatchInserters.inserter(path, config);

		this.isFriendshipDirected = isFriendshipDirected;

		//friendEdges = HashMultimap.create();
		//circleEdges = HashMultimap.create();
		personToIds = new HashMap<>();
		circleToIds = new HashMap<>();
		
		friends = CacheBuilder.newBuilder()
				.maximumSize(CACHE_MAX_SIZE)
				.build(
						new CacheLoader<Long, Set<Long>>() {
							public Set<Long> load(Long id) {
								Set<Long> friends = new HashSet<>();
								for (Long relId : inserter.getRelationshipIds(id)) {
									BatchRelationship rel = inserter.getRelationshipById(relId);
									if (rel.getType().name().equals(RelationshipTypes.FRIEND.type().name())) {
										if (isFriendshipDirected) {
											friends.add(rel.getEndNode());
										} else {
											if (rel.getStartNode() == id) {
												friends.add(rel.getEndNode());
											} else {
												friends.add(rel.getStartNode());
											}
										}
									}
								}
								return friends;
							}
						});
	}

	/**
	 * Insert the created social network graph and print out statistics. It will
	 * delete the existing folder before inserting.
	 * 
	 * @throws IOException
	 */
	public void insert() throws IOException {
		inserter.shutdown();
		
		logger.info("Number of nodes: " + personToIds.size());
		//logger.info("Number of friendships: " + friendEdges.size());
		logger.info("Number of circles: " + circleToIds.size());
		logger.info("Max size of circle: " + maxCircle);
		logger.info("Average circle size: " + ((double) totalCircleSize)
				/ circleToIds.size());
		logger.info("Average membership size: " + ((double) totalCircleSize)
				/ personToIds.size());

	}

	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#isFriend(java.lang.Object, java.lang.Object)
	 */
	@Override
	public boolean isFriend(Object from, Object to) {
		if (personExists(from) && personExists(to))
			return relationshipExists(RelationshipTypes.FRIEND.type(),
					personToIds.get(from), personToIds.get(to));
		return false;
	}

	// check if a person exists
	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#personExists(java.lang.Object)
	 */
	@Override
	public boolean personExists(Object person) {
		return personToIds.containsKey(person);
	}

	// check if a circle exists
	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#circleExists(java.lang.String)
	 */
	@Override
	public boolean circleExists(String circleName) {
		return circleToIds.containsKey(circleName);
	}

	// check if a person belongs to a circle
	// if person and/or circle do not exist, return false
	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#hasCirlce(java.lang.Object, java.lang.String)
	 */
	@Override
	public boolean hasCirlce(Object person, String circleName) {
		if (personExists(person) && circleExists(circleName)) {
			/*
			return circleEdges.containsEntry(personToIds.get(person),
					circleToIds.get(circleName));
					*/
			return relationshipExists(RelationshipTypes.CIRCLE.type(), personToIds.get(person),
					circleToIds.get(circleName));
		}

		return false;
	}

	// create a circle
	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#addCircle(java.lang.String)
	 */
	@Override
	public void addCircle(String circleName) {
		addCircle(circleName, emptyMap());
	}

	// create a circle with features
	// the created circle has label: Circle
	// the features contain name of the circle, i.e., IDENTIFIER : circleName
	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#addCircle(java.lang.String, java.util.Map)
	 */
	@Override
	public void addCircle(String circleName, Map<String, Object> features) {
		if (!circleExists(circleName)) {
			features.put(IDENTIFIER, circleName); // add circleName as property
													// IDENTIFIER
			long id = inserter.createNode(features, CIRCLE_LABEL);
			circleToIds.put(circleName, id);
		} else {
			long id = circleToIds.get(circleName);
			Map<String, Object> properties = inserter.getNodeProperties(id);
			properties.putAll(features);
			inserter.setNodeProperties(id, properties);
		}
	}

	// add a person to a circle
	// throw exception if either circle or person does not exist
	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#setCircle(java.lang.String, java.lang.Object)
	 */
	@Override
	public void setCircle(String circleName, Object person) {
		if (!personExists(person)) {
			throw new IllegalArgumentException("Node: " + person
					+ " does not exist!");
		}

		if (circleExists(circleName)) {
			// always from node to circle
			if (addRelationship(personToIds.get(person),
					circleToIds.get(circleName),
					RelationshipTypes.CIRCLE.type()) && enforceUniqueRelationships) {
				/*
				circleEdges.put(personToIds.get(person),
						circleToIds.get(circleName));
						*/
				logger.trace(person + " is now in circle:" + circleName);
				totalCircleSize++;
			}
		} else {
			throw new IllegalArgumentException("Cirlce: " + circleName
					+ " does not exist!");
		}
	}

	// create a person
	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#addPerson(java.lang.Object)
	 */
	@Override
	public void addPerson(Object person) {
		addPerson(person, emptyMap());
	}

	// create a person with features
	// the created node has label: Person
	// the features contain name of the circle, i.e., IDENTIFIER : person
	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#addPerson(java.lang.Object, java.util.Map)
	 */
	@Override
	public void addPerson(Object person, Map<String, Object> features) {
		if (!personExists(person)) {
			features.put(IDENTIFIER, person);
			long id = inserter.createNode(features, PERSON_LABEL);
			personToIds.put(person, id);
		} else {
			long id = personToIds.get(person);
			Map<String, Object> properties = inserter.getNodeProperties(id);
			properties.putAll(features);
			inserter.setNodeProperties(id, properties);
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
		if (enforceUniqueRelationships) {
			if (!relationshipExists(type, fromNodeId, toNodeId)) {
				// relationship does not exist, create new relationship
				inserter.createRelationship(fromNodeId, toNodeId, type, null);
				return true;
			} else {
				return false;
			}
		} else {
			inserter.createRelationship(fromNodeId, toNodeId, type, null);
			return true;
		}
	}

	/*
	 * Make a person a friend of another. It does nothing if one or both of
	 * people do not exist.
	 */
	/* (non-Javadoc)
	 * @see smallworld.data.inserter.exp.GraphInserter#addFriend(java.lang.Object, java.lang.Object)
	 */
	@Override
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
				RelationshipTypes.FRIEND.type()) && enforceUniqueRelationships) {
			//friendEdges.put(personToIds.get(fromNode), personToIds.get(toNode));
			logger.trace(fromNode + " and " + toNode + " are friends");
		}
	}
	
	/*
	 * Check if a relationship exists. For friendship, if it is bidirectional,
	 * check both "from => to" and "to => from." If friendship is directional,
	 * check only "from => to."
	 * 
	 * For circle relationship, always check "from (person) => to (circle)."
	 */
	private boolean relationshipExists(RelationshipType type, long fromNodeId, long toNodeId) {
		if (type == RelationshipTypes.CIRCLE.type()) {
			// direction always person => circle
			for (Long relId : inserter.getRelationshipIds(fromNodeId)) {
				BatchRelationship rel = inserter.getRelationshipById(relId);
				if (rel.getType().name().equals(RelationshipTypes.CIRCLE.type().name()) && rel.getEndNode() == toNodeId) {
					return true;
				}
			}
		} else if (type == RelationshipTypes.FRIEND.type()) {
			/*
			if (isFriendshipDirected) {
				for (Long relId : inserter.getRelationshipIds(fromNodeId)) {
					BatchRelationship rel = inserter.getRelationshipById(relId);
					if (rel.getType().name().equals(RelationshipTypes.FRIEND.type().name()) && rel.getEndNode() == toNodeId) {
						return true;
					}
				}
			} else {
				for (Long relId : inserter.getRelationshipIds(fromNodeId)) {
					BatchRelationship rel = inserter.getRelationshipById(relId);
					if (rel.getType().name().equals(RelationshipTypes.FRIEND.type().name())
							&& (rel.getEndNode() == toNodeId || rel.getStartNode() == toNodeId)) {
						return true;
					}
				}
			}
			*/
			// Use cache
			try {
				return friends.get(fromNodeId).contains(toNodeId);
			} catch (ExecutionException e) {
				logger.error(e);
			}
		}
		
		return false;
	}
	
	@Override
	public Map<String, Object> getPersonFeatures(Object person) {
		if (personExists(person)) {
			return inserter.getNodeProperties(personToIds.get(person));
		} else {
			throw new IllegalArgumentException("Person " + person + " does not exist");
		}
	}

	// utility method for deleting a folder
	private void delete(String path) throws IOException {
		Utils.delete(path);
	}
}
