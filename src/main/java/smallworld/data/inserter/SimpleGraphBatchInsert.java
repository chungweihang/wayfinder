package smallworld.data.inserter;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.Label;
import org.neo4j.unsafe.batchinsert.BatchInserter;
import org.neo4j.unsafe.batchinsert.BatchInserters;

import smallworld.data.RelationshipTypes;
import smallworld.data.query.Query;
import smallworld.util.Utils;

@Deprecated
public class SimpleGraphBatchInsert {

	private SimpleGraphBatchInsert() {
	}

	public static void insert(String path) throws IOException {
		Utils.delete(path);

		// config
		Map<String, String> config = new HashMap<String, String>();
		config.put("neostore.nodestore.db.mapped_memory", "90M");
		config.put("neostore.relationshipstore.db.mapped_memory", "3G");
		config.put("neostore.propertystore.db.mapped_memory", "50M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "100M");
		config.put("neostore.propertystore.db.arrays.mapped_memory", "0M");

		BatchInserter inserter = BatchInserters.inserter(path, config);

		for (int i = 1; i <= 7; i++) {
			inserter.createNode(i, null);
		}
		
		inserter.createRelationship(1, 2, RelationshipTypes.FRIEND.type(), null);
		inserter.createRelationship(1, 4, RelationshipTypes.FRIEND.type(), null);
		inserter.createRelationship(2, 3, RelationshipTypes.FRIEND.type(), null);
		inserter.createRelationship(2, 4, RelationshipTypes.FRIEND.type(), null);
		inserter.createRelationship(3, 4, RelationshipTypes.FRIEND.type(), null);
		
		inserter.createRelationship(4, 5, RelationshipTypes.FRIEND.type(), null);
		inserter.createRelationship(3, 5, RelationshipTypes.FRIEND.type(), null);
		
		inserter.createRelationship(5, 6, RelationshipTypes.FRIEND.type(), null);
		inserter.createRelationship(5, 7, RelationshipTypes.FRIEND.type(), null);
		inserter.createRelationship(6, 7, RelationshipTypes.FRIEND.type(), null);
		
		// circle1
		/*
		inserter.setNodeProperty(1, "circle1", 4);
		inserter.setNodeProperty(2, "circle1", 4);
		inserter.setNodeProperty(3, "circle1", 4);
		inserter.setNodeProperty(4, "circle1", 4);
		*/
		Label circle1 = DynamicLabel.label("circle1");
		inserter.setNodeLabels(1, Query.addLabel(inserter.getNodeLabels(1), circle1));
		inserter.setNodeLabels(2, Query.addLabel(inserter.getNodeLabels(2), circle1));
		inserter.setNodeLabels(3, Query.addLabel(inserter.getNodeLabels(3), circle1));
		inserter.setNodeLabels(4, Query.addLabel(inserter.getNodeLabels(4), circle1));
		
		// circle2
		/*
		inserter.setNodeProperty(4, "circle2", 2);
		inserter.setNodeProperty(5, "circle2", 2);
		*/
		Label circle2 = DynamicLabel.label("circle2");
		inserter.setNodeLabels(4, Query.addLabel(inserter.getNodeLabels(4), circle2));
		inserter.setNodeLabels(5, Query.addLabel(inserter.getNodeLabels(5), circle2));
		
		// circle3
		/*
		inserter.setNodeProperty(5, "circle3", 3);
		inserter.setNodeProperty(6, "circle3", 3);
		inserter.setNodeProperty(7, "circle3", 3);
		*/
		Label circle3 = DynamicLabel.label("circle3");
		inserter.setNodeLabels(5, Query.addLabel(inserter.getNodeLabels(5), circle3));
		inserter.setNodeLabels(6, Query.addLabel(inserter.getNodeLabels(6), circle3));
		inserter.setNodeLabels(7, Query.addLabel(inserter.getNodeLabels(7), circle3));

		inserter.shutdown();
	}

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		SimpleGraphBatchInsert.insert("neo4j/simple");
	}

}
