package smallworld.navigation.evaluator;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Arrays;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;

import smallworld.navigation.feature.FeatureBuilder;
import smallworld.util.LibSVMUtils;
import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
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

@SuppressWarnings("unused")
public class LibSVMEvaluator implements Evaluator<Integer> {
	
	private final svm_model model;
	private final FeatureBuilder features;
	
	public LibSVMEvaluator(svm_model model, FeatureBuilder features) {
		this.features = features;
		this.model = model;
	}
	
	public LibSVMEvaluator(LibSVMEvaluator another) {
		this.features = another.features;
		this.model = another.model;
	}

	/**
	 * 
	 * 
	 * 
	 * @param instance
	 * @return
	 * @throws Exception
	 */
	private Integer classify(Instance instance) throws Exception {
		svm_node[] x = LibSVMUtils.toSVMNode(instance);
		double label = LibSVMUtils.predict(model, x);
		//LibSVMUtils.printSVMNode(x);
		//System.out.println(label);
		return (int) label;
	}

	@Override
	public synchronized Integer getCost(Path path, Node target) {
		Instance instance = null;
		try {
			instance = features.getTestingInstance(path, target);
			return classify(instance);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		throw new AssertionError("Something wrong with the classifier: " + model);
	}

	@Override
	public Evaluator<Integer> copy() {
		try {
			return new LibSVMEvaluator(this);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		throw new AssertionError("failed to copy classifier!");
	}

	@Override
	public Integer getCost(Node end, Node target) {
		throw new UnsupportedOperationException();
	}
}
