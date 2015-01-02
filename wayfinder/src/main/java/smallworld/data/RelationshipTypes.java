package smallworld.data;

import org.neo4j.graphdb.DynamicRelationshipType;
import org.neo4j.graphdb.RelationshipType;

public enum RelationshipTypes {
	FRIEND(DynamicRelationshipType.withName("FRIEND")),
	//CIRCLE(DynamicRelationshipType.withName("CIRCLE")),
	KNOWS(DynamicRelationshipType.withName("KNOWS")),
	COAUTHOR(DynamicRelationshipType.withName("COAUTHOR"));
	
	private final RelationshipType type;
	
	RelationshipTypes(RelationshipType type) {
		this.type = type;
	}
	
	public RelationshipType type() { return type; }
	
	/*
	public static RelationshipType parse(String name) {
		if (name.equals("FRIENDS")) return FRIEND.type();
		else if (name.equals("KNOWS")) return KNOWS.type();
		else throw new AssertionError("Unknown relationship type: " + name);
	}
	*/
	
}
