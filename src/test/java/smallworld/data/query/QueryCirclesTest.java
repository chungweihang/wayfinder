package smallworld.data.query;

import java.io.IOException;
import java.util.Collections;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.neo4j.graphdb.DynamicLabel;

import com.google.common.collect.Lists;

import smallworld.data.inserter.exp.Neo4JInserter;
import smallworld.data.inserter.exp.SimpleGraphInserter;

public class QueryCirclesTest {
	@Rule
	public TemporaryFolder folder = new TemporaryFolder();
	
	private static QueryCircles query;
	
	@Before
	public void initialize() throws IOException {
		if (query == null) {
			Neo4JInserter inserter = new Neo4JInserter(folder.getRoot().getAbsolutePath());
			new SimpleGraphInserter(inserter);
			query = new QueryCircles(new Query(folder.getRoot().getAbsolutePath()));
		}
	}
	
	@AfterClass
	public static void cleanup() {
		if (null != query) query.shutdown();
	}
	
	@Test
	public void test() {
		System.out.println(query.getCircles(0l));
		System.out.println(query.getCircles(1l));
		System.out.println(query.getCircles(2l));
		System.out.println(query.getCircles(3l));
		System.out.println(query.getCircles(4l));
		System.out.println(query.getCircles(5l));
		System.out.println(query.getCircles(6l));
	}
	
	/*
	System.out.println(qc.getCircleLabels(q.cypherGetNode(1)));
	System.out.println(qc.getCircleLabels(q.cypherGetNode(4)));
	System.out.println(qc.sizeOfCircleLabel(DynamicLabel.label("circle1")));
	System.out.println(qc.getMaxCommonCircleLabel(q.cypherGetNode(4), q.cypherGetNode(1)));
	System.out.println(qc.getMaxCommonCircleLabel(q.cypherGetNode(4), q.cypherGetNode(5)));
	System.out.println(qc.getCommonCircleLabels(q.cypherGetNode(1), q.cypherGetNode(4)));
	System.out.println(qc.getCommonCircleLabels(q.cypherGetNode(5), q.cypherGetNode(4)));
	System.out.println(qc.getCommonCircleLabels(q.cypherGetNode(5), q.cypherGetNode(7)));
	System.out.println(qc.getCommonCircleLabels(q.cypherGetNode(5), q.cypherGetNode(1)));
	*/
}