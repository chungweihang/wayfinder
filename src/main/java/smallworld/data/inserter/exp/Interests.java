package smallworld.data.inserter.exp;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.neo4j.graphdb.Node;

import com.google.common.collect.Lists;

import smallworld.data.query.QueryCircles;
import smallworld.util.StopList;
import smallworld.util.Utils;
import edu.stanford.nlp.util.Sets;

public class Interests {
	
	public static final String INTEREST_NODE_NAME = "INTERESTS";
	static final StopList stoplist = StopList.INSTANCE;
	static Node INTEREST_NODE = null;
	
	public static void addInterests(Map<String, Object> interests, String title) {
		List<String> words = new ArrayList<>();
		// tokenize
		String[] chunks = title.split("\\W+");
		// bigram + stopwords + stemming
		for (int i = 0; i < chunks.length; i++) {
			String lowercased = chunks[i].toLowerCase();
			if (!stoplist.isStopword(lowercased)) {
				String stemmed = Utils.stem(lowercased);
				words.add(stemmed);
				if (i + 1 < chunks.length) { 
					String nextLowercased = chunks[i+1].toLowerCase();
					if (!stoplist.isStopword(nextLowercased)) {
						words.add(stemmed + "_" + Utils.stem(nextLowercased));
					}
				}
			}
		}
		
		// count
		for (String word : words) {
			if (interests.containsKey(word)) {
				interests.put(word, (Integer) interests.get(word) + 1);
			} else {
				interests.put(word, Integer.valueOf(1));
			}
		}
	}
	
	public static double proximity(Node personA, Node personB) {
		synchronized (INTEREST_NODE) {
			if (INTEREST_NODE == null) {
				INTEREST_NODE = QueryCircles.getInstance().cypherGetCirlce(INTEREST_NODE_NAME);
			}
		}
		
		Set<String> commonInterests = Sets.intersection(
				new HashSet<String>(Lists.newArrayList(personA.getPropertyKeys())), 
				new HashSet<String>(Lists.newArrayList(personB.getPropertyKeys())));
		double proximity = 0d;
		for (String interest : commonInterests) {
			proximity += (double) ((Integer) personA.getProperty(interest) * (Integer) personB.getProperty(interest)) / (Integer) INTEREST_NODE.getProperty(interest);
		}
		
		return proximity;
	}
}
