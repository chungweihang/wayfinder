package smallworld.util;

import java.io.File;
import java.io.IOException;

import org.neo4j.graphalgo.impl.util.PathImpl;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Relationship;
import org.neo4j.io.fs.FileUtils;
import org.tartarus.snowball.ext.englishStemmer;

import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.core.converters.LibSVMSaver;

public class Utils {
	/**
	 * Utility method for composing a path.
	 * 
	 * @param start
	 * @param rels
	 * @return
	 */
	public static Path toPath(Node start, Iterable<Relationship> rels) {
		PathImpl.Builder builder = new PathImpl.Builder(start);
		
		if (rels != null) {
			for (Relationship rel : rels) {
				builder = builder.push(rel);
			}
		}
		return builder.build();
	}
	
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
	
	private static final englishStemmer stemmer = new englishStemmer();
	public static String stem(String text) {
		stemmer.setCurrent(text);
		stemmer.stem();
		return stemmer.getCurrent();
	}
	
	// utility method for deleting a folder
	public static void delete(String path) throws IOException {
		FileUtils.deleteRecursively(new File(path));
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		//toLibSVM("facebook-10x10.arff", "facebook-10x10.libsvm");
		toLibSVM("facebook-100x100.arff", "facebook-100x100.libsvm");
	}

}
