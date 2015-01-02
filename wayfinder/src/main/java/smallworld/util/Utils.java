package smallworld.util;

import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.core.converters.LibSVMSaver;

public class Utils {

	public static void toLibSVM(String arff, String output) {
		try {
			Instances instances = new DataSource(arff).getDataSet();
			LibSVMSaver saver = new LibSVMSaver();
			saver.setOptions(weka.core.Utils.splitOptions("-o " + output));
			saver.setInstances(instances);
			saver.writeBatch();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//toLibSVM("facebook-10x10.arff", "facebook-10x10.libsvm");
		toLibSVM("facebook-100x100.arff", "facebook-100x100.libsvm");
	}

}
