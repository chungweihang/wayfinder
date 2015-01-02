package smallworld.navigation.feature;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;

import smallworld.Constants;
import smallworld.data.query.Query;
import smallworld.data.query.QueryCircles;
import weka.core.Attribute;
import weka.core.FastVector;
import weka.core.Instance;
import weka.core.Instances;

/**
 * A collection of features.
 * 
 * This class helps machine learning approaches create instances.
 * It provides a builder to quickly add features to it.
 * 
 * The features of the instances can be either scaled or not.
 * This depends on if scaling is set to true or false.
 * 
 * This class maintains a dataset that contains multiple instances.
 * It provides methods to get an instance, or add an instance to 
 * the dataset.
 * 
 * This class also provides static methods to create commonly
 * used features.
 * 
 * @author chang
 *
 */
public class FeatureBuilder {
	
	// Control whether features get scaled or not
	private static boolean scaling = false;
	
	private List<Feature<Double>> features;
	private FastVector attributes;
	private Instances data = null;
	
	public FeatureBuilder() {
		features = new ArrayList<Feature<Double>>();
		attributes = new FastVector();
	}
	/*
	public FeatureBuilder(FeatureBuilder another) {
		features = new ArrayList<Feature<Double>>(another.features);
		attributes = new FastVector();
		attributes.appendElements(another.attributes);
	}
	*/
	public static boolean isScaling() {
		return scaling;
	}

	public static void setScaling(boolean scaling) {
		FeatureBuilder.scaling = scaling;
	}

	
	public FeatureBuilder addFeature(Feature<Double> feature) {
		features.add(feature);
		attributes.addElement(new Attribute(feature.getName()));
		return this;
	}
	
	public FeatureBuilder addClassFeature(Feature<Double> feature) {
		features.add(feature);
		FastVector classes = new FastVector();
		classes.addElement("0");
		classes.addElement("1");
		attributes.addElement(new Attribute(feature.getName(), classes));
		return this;
	}
	
	public List<Feature<Double>> features() { return features; }
	
	/**
	 * Get the dataset
	 * 
	 * @return
	 */
	public Instances getInstances() { 
		if (data == null) {
			data = new Instances("data", attributes, 0);
			data.setClassIndex(attributes.size() - 1);
		}
		
		return data;
	}
	
	public Instance getTestingInstance(Path path, Node target) {
		
		Instance instance = new Instance(features.size());
		instance.setDataset(getInstances());
		
		for (int i = 0; i < features.size() - 1; i++) {
			Feature<Double> feature = features.get(i);
			double value = feature.getFeature(path, target);
			instance.setValue((Attribute)attributes.elementAt(i), value);
			
			/*
			// set weight
			if (feature.getName().equals("PathLength")) {
				instance.setWeight(1d/value);
			}
			*/
		}
		
		return instance;
	}
	
	public synchronized void addTrainingInstance(Instance i) {
		data.add(i);
	}
	
	public synchronized Instance getTrainingInstance(Path path, Node target) {
	
		Instance instance = getTestingInstance(path, target);
		instance.setValue((Attribute)attributes.lastElement(), features.get(features.size() - 1).getFeature(path, target));
		
		//instance.setDataset(data);
		data.add(instance);
		
		return instance;
	}
	
	public static Feature<Double> getPathLengthFeature(String dataset) {
		
		final double max;
		if (dataset.equals("facebook")) max = 350d;
		else max = 1000d;
		
		return new Feature<Double>() {
			@Override
			public Double getFeature(Path path, Node target) {
				int count = path.length();
				if (scaling) {
					if (count > max) System.err.println("PathLength feature " + count + " exceed max: " + max);
					return ((double)count) / max;
				} else {
					return (double) count;
				}
			}

			@Override
			public String getName() {
				return "PathLength";
			}
		};
	}
	
	public static Feature<Double> getEndNodeDegreeFeature(String dataset, final RelationshipType type, final Direction direction) {
		
		final double max;
		if (dataset.equals("facebook")) max = 1100d;
		else max = 5000d;
		
		return new Feature<Double>() {
			@Override
			public Double getFeature(Path path, Node target) {
				int count = Query.getDegree(path.endNode(), type, direction);
				if (scaling) {
					if (count > max) System.err.println("EndNodeDegree feature " + count + " exceed max: " + max);
					return ((double)count) / max;
				} else {
					return (double) count;
				}
			}

			@Override
			public String getName() {
				return "EndNodeDegree";
			}
		};
	}
	
	public static Feature<Double> getParentNodeDegreeFeature(String dataset, final RelationshipType type, final Direction direction) {
		final double max;
		if (dataset.equals("facebook")) max = 1100d;
		else max = 5000d;
		
		return new Feature<Double>() {
			@Override
			public Double getFeature(Path path, Node target) {
				int count = Query.getDegree(path.lastRelationship().getOtherNode(path.endNode()), type, direction);
				if (scaling) {
					if (count > max) System.err.println("ParentNodeDegree feature " + count + " exceed max: " + max);
					return ((double)count) / max;
				} else {
					return (double) count;
				}
			}

			@Override
			public String getName() {
				return "ParentNodeDegree";
			}
		};
	}
	
	public static Feature<Double> getCommonCirclesWithTarget(String dataset) {
		final double max;
		if (dataset.equals("facebook")) max = 20d;
		else max = 100d;
		
		return new Feature<Double>() {
			@Override
			public Double getFeature(Path path, Node target) {
				int count = QueryCircles.getCommonCircles(path.endNode(), target).size();
				if (scaling) {
					if (count > max) System.err.println("CommonCirclesWithTarget feature " + count + " exceed max: " + max);
					return ((double)count) / max;
				} else {
					return (double) count;
				}
			}

			@Override
			public String getName() {
				return "CommonCirclesWithTarget";
			}
		};
	}

	public static Feature<Double> getCommonCirclesWithParent(String dataset) {
		final double max;
		if (dataset.equals("facebook")) max = 20d;
		else max = 100d;
		
		return new Feature<Double>() {
			@Override
			public Double getFeature(Path path, Node target) {
				int count = QueryCircles.getCommonCircles(path.lastRelationship().getOtherNode(path.endNode()), path.endNode()).size();
				if (scaling) {
					if (count > max) System.err.println("CommonCirclesWithParent feature " + count + " exceed max: " + max);
					return ((double)count) / max;
				} else {
					return (double) count;
				}
			}

			@Override
			public String getName() {
				return "CommonCirclesWithParent";
			}
		};
	}
	
	public static Feature<Double> getCommonCirclesBetweenParentTarget(String dataset) {
		final double max;
		if (dataset.equals("facebook")) max = 20d;
		else {
			max = 100d;
		}
		
		return new Feature<Double>() {
			@Override
			public Double getFeature(Path path, Node target) {
				int count = QueryCircles.getCommonCircles(path.lastRelationship().getOtherNode(path.endNode()), target).size();
				if (scaling) {
					if (count > max) System.err.println("CommonCirclesBetweenParentTarget feature " + count + " exceed max: " + max);
					return ((double)count) / max;
				} else {
					return (double) count;
				}
			}

			@Override
			public String getName() {
				return "CommonCirclesBetweenParentTarget";
			}
		};
	}
	
	public static Feature<Double> getMinCommonCircleWithTarget(String dataset) {
		final double max;
		if (dataset.equals("facebook")) max = 310;
		else {
			max = 1000d;
		}
		
		return new Feature<Double>() {
			@Override
			public Double getFeature(Path path, Node target) {
				double size = QueryCircles.getMinCommonCircle(path.lastRelationship().getOtherNode(path.endNode()), target);
				if (size == Integer.MAX_VALUE) size = max;
				if (scaling) {
					if (size > max) System.err.println("MinCommonCircleWithTarget feature " + size + " exceed max: " + max);
					return size / max;
				} else {
					return size;
				}
			}

			@Override
			public String getName() {
				return "MinCommonCircleWithTarget";
			}
		};
	}
	
	public static Feature<Double> getMaxCommonCircleWithTarget(String dataset) {
		final double max;
		if (dataset.equals("facebook")) max = 1000d;
		else {
			max = 1000d;
		}
		
		return new Feature<Double>() {
			@Override
			public Double getFeature(Path path, Node target) {
				int size = QueryCircles.getMaxCommonCircle(path.lastRelationship().getOtherNode(path.endNode()), target);
				if (scaling) {
					if (size > max) System.err.println("MaxCommonCircleWithTarget feature " + size + " exceed max: " + max);
					return ((double)size) / max;
				} else {
					return (double) size;
				}
			}

			@Override
			public String getName() {
				return "MaxCommonCircleWithTarget";
			}
		};
	}
}
