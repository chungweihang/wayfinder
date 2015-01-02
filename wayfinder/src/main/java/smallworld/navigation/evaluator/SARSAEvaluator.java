package smallworld.navigation.evaluator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.Traversal;

import smallworld.ConcurrentMain;
import smallworld.data.RelationshipTypes;
import smallworld.navigation.AbstractNavigation;
import smallworld.navigation.PrioritizedDFSNavigation;
import smallworld.navigation.feature.DistanceMeasure;
import smallworld.navigation.feature.FeatureBuilder;

public class SARSAEvaluator implements Evaluator<Integer> {
	
	private FeatureBuilder builder;
	private double[] w;
	private final double alpha = 0.8d; // learning rate
	
	public SARSAEvaluator(FeatureBuilder builder) {
		this.builder = builder;
		this.w = new double[builder.features().size()];
		Arrays.fill(this.w, 1d / (double) builder.features().size());
	}
	
	public void loadParameters(String filename) {
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(filename));
			
			String line = reader.readLine();
			String[] tokens = line.split(",");
			if (tokens.length != w.length) System.err.println("parameter missing, expected " + w.length + ", actual " + tokens.length);
			else {
				for (int i = 0; i < w.length; i++) {
					w[i] = Double.parseDouble(tokens[i]);
				}
			}
		} catch (NumberFormatException e) {
			e.printStackTrace();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != reader) try { reader.close(); } catch (Exception e) {}
		}
	}
	
	public void saveParameters(String filename) {
		BufferedWriter writer = null;
		try {
			writer = new BufferedWriter(new FileWriter(filename));
			
			for (int i = 0; i < w.length; i++) {
				if (i > 0) writer.write(",");
				writer.write("" + w[i]);
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != writer) try { writer.close(); } catch (Exception e) {}
		}
	}
	
	private SARSAEvaluator(SARSAEvaluator another) {
		this.builder = another.builder;
		this.w = another.w;
	}
	
	public void update(Path path, Node target) {
		
		//System.out.println("[DEBUG] current: " + path);
		if (path.length() == 1) return;
		
		PathImpl.Builder builder = new PathImpl.Builder(path.startNode());
		int length = 0;
		if (path.relationships() != null) {
			for (Relationship rel : path.relationships()) {
				if (++length < path.length()) {
					builder = builder.push(rel);
				}
			}
		}
		
		Path previous = builder.build();
		//System.out.println("[DEBUG] previous: " + previous);
		
		double previousV = V(previous, target);
		double currentV = V(path, target);
		
		//System.out.println("[DEBUG] v: " + previousV + " : " + currentV);
		
		double r = 0d;
		if (!previous.endNode().equals(target)) r = -1d;
		else r = 1d;
		
		double parameter = alpha * (r + currentV - previousV);
		double[] f = f(previous, target);
		
		//System.out.println("[DEBUG] w1: " + Arrays.toString(w));
		
		for (int i = 0; i < w.length; i++) {
			w[i] += parameter * f[i];
		}
		
		//System.out.println("[DEBUG] w2: " + Arrays.toString(w));
	}
	
	
	
	public double[] f(Path path, Node target) {
		double[] f = new double[builder.features().size()];
		
		for (int i = 0; i < f.length; i++) {
			f[i] = builder.features().get(i).getFeature(path, target);
		}
		
		return f;
	}
	
	public double V(Path path, Node target) {
		double[] f = f(path, target);
		
		double v = 0d;
		for (int i = 0; i < w.length; i++) {
			v += w[i] * f[i];
		}
		
		return v;
	}

	@Override
	public Evaluator<Integer> copy() {
		return new SARSAEvaluator(this);
	}

	@Override
	public Integer getCost(Path path, Node target) {
		return (int) (0 - Math.round(V(path, target) * 1000d));
	}

	public static void main(String[] args) throws IOException {
		String dataset = "facebook";
		
		RelationshipType type = RelationshipTypes.FRIEND.type();
		Direction direction = Direction.BOTH;
		//int depth = 20;
		
		DistanceMeasure distanceFeature = new DistanceMeasure(dataset, type, direction);
		FeatureBuilder.setScaling(true);
		FeatureBuilder features = new FeatureBuilder()
			.addFeature(FeatureBuilder.getCommonCirclesWithParent(dataset))
			.addFeature(FeatureBuilder.getCommonCirclesWithTarget(dataset))
			.addFeature(FeatureBuilder.getCommonCirclesBetweenParentTarget(dataset))
			.addFeature(FeatureBuilder.getEndNodeDegreeFeature(dataset, type, direction))
			.addFeature(FeatureBuilder.getParentNodeDegreeFeature(dataset, type, direction))
			.addFeature(FeatureBuilder.getPathLengthFeature(dataset));
			//.addFeature(FeatureBuilder.getDistanceLabel(type, direction, depth));

		AbstractNavigation.setDistanceMeasure(distanceFeature);
		SARSAEvaluator evaluator = new SARSAEvaluator(features);
		evaluator.loadParameters(dataset + ".sarsa");
		
		/*
		Instance instance = new Instance(1, new double[] {0d, 0d, 100d, 100d, 100d, 0});
		instance.setDataset(features.getInstances());
		System.out.println(evaluator.classify(instance));
		
		instance = new Instance(1, new double[] {0d, 0d, 100d, 100d, 100d, 1});
		instance.setDataset(features.getInstances());
		System.out.println(evaluator.classify(instance));
		*/
		
		new ConcurrentMain(new PrioritizedDFSNavigation(Traversal.pathExpanderForTypes(type, direction), evaluator), "neo4j/" + dataset, 100, 0, dataset + ".sarsa.sample.log");
		//distanceFeature.saveCache();

	}

	@Override
	public Integer getCost(Node end, Node target) {
		throw new UnsupportedOperationException();
	}
}
