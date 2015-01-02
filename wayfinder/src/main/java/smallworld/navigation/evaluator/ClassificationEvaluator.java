package smallworld.navigation.evaluator;

import java.io.PrintStream;
import java.util.Arrays;

import libsvm.svm;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.Traversal;

import smallworld.ConcurrentMain;
import smallworld.data.RelationshipTypes;
import smallworld.navigation.AbstractNavigation;
import smallworld.navigation.PrioritizedDFSNavigation;
import smallworld.navigation.feature.DistanceMeasure;
import smallworld.navigation.feature.FeatureBuilder;
import weka.classifiers.Classifier;
import weka.classifiers.functions.LibLINEAR;
import weka.classifiers.functions.LinearRegression;
import weka.classifiers.functions.Logistic;
import weka.classifiers.functions.SimpleLogistic;
import weka.classifiers.trees.J48;
import weka.core.Instance;
import weka.core.Instances;
import weka.core.Utils;
import weka.core.converters.ConverterUtils.DataSource;
//import wlsvm.WLSVM;

@SuppressWarnings("unused")
public class ClassificationEvaluator implements Evaluator<Integer> {
	
	private final Instances data;
	private final Classifier classifier;
	private final FeatureBuilder builder;
	
	public ClassificationEvaluator(Classifier classifier, FeatureBuilder builder, Instances data) throws Exception {
		//this.builder = new FeatureBuilder(builder);
		this.builder = builder;
		this.data = data;
		this.data.setClassIndex(data.numAttributes() - 1);
		
		this.classifier = classifier;
		this.classifier.buildClassifier(this.data);
	}
	
	public void printParameters() {
		if (classifier instanceof Logistic) {
			double[][] coefficients = ((Logistic) classifier).coefficients();

			System.out.println("== LOGISTIC COEFFICIENTS ==");
			for (double[] d : coefficients) {
				System.out.println(Arrays.toString(d));
			}
			System.out.println("===========================");
		}
	}
	
	public ClassificationEvaluator(ClassificationEvaluator another) throws Exception {
		this.builder = another.builder;
		this.data = another.data;
		this.data.setClassIndex(data.numAttributes() - 1);
		
		this.classifier = Classifier.makeCopy(another.classifier);
	}
	
	public ClassificationEvaluator(Classifier classifier, FeatureBuilder builder, String arff) throws Exception {
		this(classifier, builder, new DataSource(arff).getDataSet());
	}
	
	@Override
	public Evaluator<Integer> copy() {
		try {
			return new ClassificationEvaluator(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		throw new AssertionError("failed to copy classifier!");
	}

	/**
	 * 
	 * 
	 * 
	 * @param instance
	 * @return
	 * @throws Exception
	 */
	private double classify(Instance instance) throws Exception {
		return classifier.distributionForInstance(instance)[1];
	}

	@Override
	public Integer getCost(Path path, Node target) {
		Instance instance = null;
		try {
			instance = builder.getTestingInstance(path, target);
			return 0 - (int) Math.round(classify(instance) * 1000d);
		} catch (Exception e) {
			System.out.println(instance);
			e.printStackTrace();
		}
		
		throw new AssertionError("Something wrong with the classifier: " + classifier);
	}

	@Override
	public Integer getCost(Node end, Node target) {
		throw new UnsupportedOperationException();
	}
	
	/*
	public static void main(String[] args) throws Exception {
		
		//String dataset = "twitter"; Direction direction = Direction.OUTGOING;
		//String dataset = "gplus"; Direction direction = Direction.OUTGOING;
		String dataset = "facebook"; Direction direction = Direction.BOTH;
		
		RelationshipType type = RelationshipTypes.FRIEND.type();
		
		DistanceMeasure distanceFeature = new DistanceMeasure(dataset, type, direction);
		FeatureBuilder features = new FeatureBuilder()
			.addFeature(FeatureBuilder.getCommonCirclesWithParent(dataset))
			.addFeature(FeatureBuilder.getCommonCirclesWithTarget(dataset))
			.addFeature(FeatureBuilder.getCommonCirclesBetweenParentTarget(dataset))
			.addFeature(FeatureBuilder.getMinCommonCircleWithTarget(dataset))
			.addFeature(FeatureBuilder.getMaxCommonCircleWithTarget(dataset))
			.addFeature(FeatureBuilder.getEndNodeDegreeFeature(dataset, type, direction))
			.addFeature(FeatureBuilder.getParentNodeDegreeFeature(dataset, type, direction))
			.addFeature(FeatureBuilder.getPathLengthFeature(dataset))
			.addClassFeature(distanceFeature);
			
		AbstractNavigation.setDistanceMeasure(distanceFeature);

		//Classifier classifier = new LibLINEAR(); classifier.setOptions(Utils.splitOptions("-S 0 -Z -D")); String log = "liblinear";
		//Classifier classifier = new WLSVM(); classifier.setOptions(Utils.splitOptions("-S 0 -K 2 -Z 1 -c 128 -g 2")); String log = "svm";
		//Classifier classifier = new J48(); String log = "j48";
		Classifier classifier = new Logistic(); String log = "logistic";
		//Classifier classifier = new SimpleLogistic(); String log = "simplelogistic";
				
		MachineLearningEvaluator evaluator = 
				new MachineLearningEvaluator(
						classifier, features, dataset + "-10x10.arff");
		
		//Instance instance = new Instance(1, new double[] {0d, 0d, 1d, 100d, 100d, 100d, 0});
		//instance.setDataset(features.getInstances());
		//System.out.println(evaluator.classify(instance));
		
		new ConcurrentMain(new PrioritizedDFSNavigation(Traversal.pathExpanderForTypes(type, direction), evaluator), "neo4j/" + dataset, 100, new PrintStream(dataset + "." + log + ".100.log"));
		
		distanceFeature.saveCache();
	}
	*/
}
