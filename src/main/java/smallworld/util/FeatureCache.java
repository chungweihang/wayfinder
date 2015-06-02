package smallworld.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Hashtable;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A cache that saves distances between all the pairs in the graph.
 * Use "getInstance" rather than the constructor for initialization.
 * 
 * @author chang
 *
 */
public class FeatureCache {
	
	private final static String FOLDER = "cache";
	
	private String path;
	private ConcurrentMap<Long, Map<Long, Integer>> cache = null;
	
	private FeatureCache(String path) {
		this.path = path;
		
		ObjectInputStream in = null;
		try {
			in = new ObjectInputStream(new FileInputStream(path));
			@SuppressWarnings("unchecked")
			Map<Long, Map<Long, Integer>> readObject = (Map<Long, Map<Long, Integer>>) in.readObject();
			cache = new ConcurrentHashMap<Long, Map<Long, Integer>>(readObject);
		} catch (FileNotFoundException e) {
			System.err.println(e);
			cache = new ConcurrentHashMap<Long, Map<Long, Integer>>();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (null != in) try { in.close(); } catch (Exception e) {}
		}
	}
	
	public Integer getFeature(long source, long target) {
		if (cache.containsKey(target)) return cache.get(target).get(source);
		return null;
	}
	
	public void setFeature(long source, long target, int feature) {
		//if (!cache.containsKey(target)) cache.put(target, new Hashtable<Long, Integer>());
		//cache.get(target).put(source, feature);
		cache.putIfAbsent(target, new ConcurrentHashMap<Long, Integer>());
		((ConcurrentMap<Long, Integer>)cache.get(target)).putIfAbsent(source, feature);
	}
	
	public void save() {
		ObjectOutputStream out = null;
		Map<Long, Map<Long, Integer>> writeObject = new Hashtable<Long, Map<Long, Integer>>(cache);
		try {
			out = new ObjectOutputStream(new FileOutputStream(path));
			out.writeObject(writeObject);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != out) try { out.close(); } catch (Exception e) {}
		}
	}
	
	public static FeatureCache getInstance(String dataset) {
		return new FeatureCache(FOLDER + File.separator + dataset + ".obj");
	}
}
