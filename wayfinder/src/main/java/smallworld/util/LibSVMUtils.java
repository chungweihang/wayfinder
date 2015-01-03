package smallworld.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.Vector;

import libsvm.svm;
import libsvm.svm_model;
import libsvm.svm_node;
import libsvm.svm_parameter;
import libsvm.svm_problem;

import org.junit.Test;

import weka.core.Instance;
import weka.core.Instances;
import weka.core.converters.ArffLoader;
import weka.core.converters.LibSVMSaver;

public class LibSVMUtils {
	
	public static svm_node[] toSVMNode(Instance instance) {
		List<svm_node> nodes = new ArrayList<>(instance.numAttributes());
		for (int i = 0; i < instance.numAttributes(); i++) {
			double value = instance.value(i);
			if (!Double.isNaN(value) && value != 0d) {
				svm_node node = new svm_node();
				node.index = i + 1;
				node.value = value;
				nodes.add(node);
			}
		}
		return nodes.toArray(new svm_node[nodes.size()]);
	}
	
	public static void arffToLibSVM(String arff, String libSVM) throws IOException {
		ArffLoader loader = new ArffLoader();
		loader.setSource(new File(arff));
		Instances instances = loader.getDataSet();
		
		LibSVMSaver saver = new LibSVMSaver();
		saver.setFile(new File(libSVM));
		saver.setInstances(instances);
		saver.writeBatch();
	}
	
	public static double[] predictProbabilities(svm_model model, svm_node[] x) {
		double[] probabilities = new double[2];
		/*double label = */svm.svm_predict_probability(model, x, probabilities);
		//return label;
		return probabilities;
	}

	public static double predict(svm_model model, svm_node[] x) {
		double[] probabilities = new double[2];
		double label = svm.svm_predict_probability(model, x, probabilities);
		//System.out.println(label + " " + Arrays.toString(probabilities));
		return label;
		//return probabilities;
	}

	public static svm_model train(String trainingFile) {
		svm_parameter param = new svm_parameter();
		// default values
		param.svm_type = svm_parameter.C_SVC;
		param.kernel_type = svm_parameter.RBF;
		param.degree = 3;
		param.gamma = 0;	// 1/num_features
		param.coef0 = 0;
		param.nu = 0.5;
		param.cache_size = 100;
		param.C = 1;
		param.eps = 1e-3;
		param.p = 0.1;
		param.shrinking = 1;
		param.probability = 0;
		param.nr_weight = 0;
		param.weight_label = new int[0];
		param.weight = new double[0];
		
		svm_model model = null;
		try {
			svm_problem problem = read_problem(trainingFile, param);
			model = svm.svm_train(problem, param);
			svm.svm_save_model(trainingFile + ".model", model);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return model;
	}
	
	private static svm_problem read_problem(String trainFile, svm_parameter param) throws IOException
	{
		BufferedReader fp = new BufferedReader(new FileReader(trainFile));
		Vector<Double> vy = new Vector<Double>();
		Vector<svm_node[]> vx = new Vector<svm_node[]>();
		int max_index = 0;

		while(true)
		{
			String line = fp.readLine();
			if(line == null) break;

			StringTokenizer st = new StringTokenizer(line," \t\n\r\f:");

			vy.addElement(Double.valueOf(st.nextToken()).doubleValue());
			int m = st.countTokens()/2;
			svm_node[] x = new svm_node[m];
			for(int j=0;j<m;j++)
			{
				x[j] = new svm_node();
				x[j].index = Integer.valueOf(st.nextToken()).intValue();
				x[j].value = Double.valueOf(st.nextToken()).doubleValue();
			}
			if(m>0) max_index = Math.max(max_index, x[m-1].index);
			vx.addElement(x);
		}

		svm_problem prob = new svm_problem();
		prob.l = vy.size();
		prob.x = new svm_node[prob.l][];
		for(int i=0;i<prob.l;i++)
			prob.x[i] = vx.elementAt(i);
		prob.y = new double[prob.l];
		for(int i=0;i<prob.l;i++)
			prob.y[i] = vy.elementAt(i);

		if(param.gamma == 0 && max_index > 0)
			param.gamma = 1.0/max_index;

		if(param.kernel_type == svm_parameter.PRECOMPUTED)
			for(int i=0;i<prob.l;i++)
			{
				if (prob.x[i][0].index != 0)
				{
					System.err.print("Wrong kernel matrix: first column must be 0:sample_serial_number\n");
					System.exit(1);
				}
				if ((int)prob.x[i][0].value <= 0 || (int)prob.x[i][0].value > max_index)
				{
					System.err.print("Wrong input format: sample_serial_number out of range\n");
					System.exit(1);
				}
			}

		fp.close();
		
		return prob;
	}
	
	public static void predictFromFile(svm_model model, String testFile) {
		int correct = 0;
		int total = 0;
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new FileReader(testFile));
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				StringTokenizer tokens = new StringTokenizer(line, " \t\n\r\f:");
				double truth = Double.valueOf(tokens.nextToken()).doubleValue();
				int m = tokens.countTokens() / 2;
				svm_node[] x = new svm_node[m];
				for (int i = 0; i < m; i++) {
					x[i] = new svm_node();
					x[i].index = Integer.valueOf(tokens.nextToken()).intValue();
					x[i].value = Double.valueOf(tokens.nextToken()).doubleValue();
				}
				
				total++;
				double label = predict(model, x);
				if (Double.compare(truth, label) == 0) {
					correct ++;
				}
			}
			
			System.out.println((double) correct / total);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != reader) try { reader.close(); } catch (IOException ignored) {}
		}
	}
	
	public static void printSVMNode(svm_node[] x) {
		for (svm_node node : x) {
			System.out.print(node.index + ":" + node.value + " ");
		}
		System.out.println();
	}
	
	public static class UnitTest {
		@Test
		public void testLibSVM() throws IOException {
			svm_model model = train("svmguide1");
			predictFromFile(model, "svmguide1.t");
			model = svm.svm_load_model("svmguide1.model");
			predictFromFile(model, "svmguide1.t");
		}
		
		@Test
		public void testTrain() {
			svm_model model = train("facebook-10.libsvm");
			predictFromFile(model, "facebook-10.libsvm");
		}
		
		@Test
		public void testInstance() throws IOException {
			Instance i = new Instance(10);
			
			i.setValue(0, 5d);
			i.setValue(1, 1d);
			i.setValue(2, 15d);
			i.setValue(3, 1045d);
			
			svm_node[] x = toSVMNode(i);
			printSVMNode(x);
			
			svm_model model = svm.svm_load_model("facebook-10.libsvm.selected.model");
			System.out.println(predict(model, x));
		}
	}
}
