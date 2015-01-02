package smallworld.data.query;

import java.util.Iterator;

import org.neo4j.cypher.javacompat.ExecutionResult;

public class QueryLabels {

	private Query query;
	
	public QueryLabels(Query query) {
		this.query = query;
	}
	
	public QueryLabels(String path) {
		this(new Query(path));
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Query q = new Query("neo4j/simple");
		QueryLabels ql = new QueryLabels(q);
		System.out.println(ql.sizeOfCircle("circle3"));
	}
}
