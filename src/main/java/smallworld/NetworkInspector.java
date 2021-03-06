package smallworld;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Iterator;

import org.neo4j.graphdb.Node;

import smallworld.data.query.Query;
import smallworld.data.query.QueryCircles;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

public class NetworkInspector {

	public static void circleDistribution(String dataset) throws FileNotFoundException {
		Query q = new Query("neo4j/" + dataset);
		QueryCircles qc = new QueryCircles(q);
		
		Multiset<Node> circleSizes = HashMultiset.create();
		Multiset<Integer> peopleSizes = HashMultiset.create();
		
		Long[] nodes = q.cypherGetAllNodes();
		System.err.println("number of nodes: " + nodes.length);
		
		for (Long nid : nodes) {
			int circleCount = 0;
			Iterator<Node> circles = qc.getCircles(q.cypherGetNode(nid)).iterator();
			while (circles.hasNext()) {
				circleCount++;
				Node circle = circles.next();
				circleSizes.add(circle);
			}
			
			peopleSizes.add(circleCount);
		}
		
		Multiset<Integer> dists = HashMultiset.create();
		for (Node circle : circleSizes.elementSet()) {
			dists.add(circleSizes.count(circle));
		}
		
		PrintWriter circleDistributionWriter = new PrintWriter(dataset + "-circles.csv");
		circleDistributionWriter.println("Circle Size,Count");
		for (Integer size : dists.elementSet()) {
			circleDistributionWriter.println("" + size + "," + dists.count(size));
		}
		circleDistributionWriter.close();
		
		PrintWriter peopleDistributionWriter = new PrintWriter(dataset + "-people.csv");
		peopleDistributionWriter.println("Circle Size,Count");
		for (Integer size : peopleSizes.elementSet()) {
			peopleDistributionWriter.println("" + size + "," + peopleSizes.count(size));
		}
		peopleDistributionWriter.close();
	}
	
	/**
	 * @param args
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException {
		/*
		circleDistribution("facebook");
		circleDistribution("dblp");
		circleDistribution("dblp-inproceedings");
		circleDistribution("twitter");
		circleDistribution("gplus");
		circleDistribution("amazon");
		circleDistribution("youtube");
		*/
		circleDistribution("msacademy-circlesize");
	}

}
