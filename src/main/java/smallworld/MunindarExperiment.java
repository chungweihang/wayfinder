package smallworld;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import smallworld.data.query.Query;
import smallworld.util.Utils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;

public class MunindarExperiment {

	static final Multimap<Integer, Double> degrees = HashMultimap.create();
	static final Multimap<Integer, Double> betweennessCentralities = HashMultimap.create();
	static final Multimap<Integer, Double> clusteringCoefficients = HashMultimap.create();
	static final Random rand = new Random(0);
	
	private static Double mean(Collection<Double> values) {
		double sum = 0d;
		if (values.isEmpty()) return sum;
		
		for (Double v : values) {
			sum += v;
		}
		
		return sum / values.size();
	}
	
	private static void collectStatistics(GraphDatabaseService service, Path path) {
		int depth = 0;
		try (Transaction tx = service.beginTx()) {
			for (Node node : path.nodes()) {
				depth++;
				degrees.put(depth, Double.valueOf(node.getDegree()));
				betweennessCentralities.put(depth, Double.valueOf((Integer) node.getProperty("betweenness_centrality", 0)));
				clusteringCoefficients.put(depth, (Double) node.getProperty("clustering_coefficient", Double.valueOf(0)));
			}
		}
	}
	
	public static Path randomWalk(GraphDatabaseService service, Node source, int maxDepth) {
		Path p = Utils.toPath(source, null);
		Set<Long> visited = new HashSet<>(maxDepth);
		visited.add(source.getId());
		
		try (Transaction tx = service.beginTx()) {
			while (p.length() < maxDepth) {
				Node endNode = p.endNode();
				List<Relationship> rels = Lists.newArrayList(endNode.getRelationships());
				
				if (rels.isEmpty()) {
					break;
				} else {
					
					
					Relationship rel = rels.remove(rand.nextInt(rels.size()));
					Long nextNodeId = rel.getOtherNode(endNode).getId();
				
					while (visited.contains(nextNodeId)) {
						if (!rels.isEmpty()) {
							rel = rels.remove(rand.nextInt(rels.size()));
							nextNodeId = rel.getOtherNode(endNode).getId();
						} else {
							nextNodeId = null;
						}
					}
					
					if (nextNodeId == null) break;
					
					visited.contains(nextNodeId);
					List<Relationship> pathRels = Lists.newArrayList(p.relationships());
					pathRels.add(rel);
					p = Utils.toPath(source, pathRels);
				}
			}
		}
		
		return p;
	}
	
	public static void main(String[] args) throws ParseException {
		// parsing arguments
		Options options = new Options();
		CommandLineParser parser = new BasicParser();
		options.addOption("neo4j", true, "Neo4j graph name (assume under ./neo4j)");
		options.addOption("n", true, "number of paths");
		options.addOption("depth", true, "max depth of path");
		CommandLine cmd = parser.parse(options, args);
		
		if (!cmd.hasOption("neo4j")) {
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp(MunindarExperiment.class.getName(), options);
			System.exit(1);
		}
		
		final String graphName = cmd.getOptionValue("neo4j");
		final int numberOfPaths = Integer.valueOf(cmd.getOptionValue("n", "100"));
		final int maxDepth = Integer.valueOf(cmd.getOptionValue("depth", "20"));
		
		// picking random starting vertices
		Query query = new Query("neo4j/" + graphName);
		Long[] nodeIds = query.cypherGetAllNodes();
		
		int n = Math.min(nodeIds.length, numberOfPaths);
		
		Set<Long> randomNodeIds = new HashSet<Long>(n);
		if (n == nodeIds.length) {
			randomNodeIds.addAll(Arrays.asList(nodeIds));
		} else {
			while (randomNodeIds.size() < n) {
				Long id = nodeIds[rand.nextInt(nodeIds.length)];
				if (!randomNodeIds.contains(id)) {
					randomNodeIds.add(id);
				}
			}
		}
		
		// random walk path and collect numbers
		for (Long id : randomNodeIds) {
			Node source = query.cypherGetNode(id);
			Path path = randomWalk(query.getGraphDatabaseService(), source, maxDepth);
			collectStatistics(query.getGraphDatabaseService(), path);
		}
		
		// output numbers for each depth
		for (int d = 1; d <= maxDepth; d++) {
			System.out.println("d," + d + "," + mean(degrees.get(d)));
			System.out.println("bc," + d + "," + mean(betweennessCentralities.get(d)));
			System.out.println("cc," + d + "," + mean(clusteringCoefficients.get(d)));
		}
	}

}
