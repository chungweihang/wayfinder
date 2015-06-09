package smallworld.data.inserter.exp;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import smallworld.util.StopList;
import smallworld.util.Utils;

public class Interests {
	
	static final StopList stoplist = StopList.INSTANCE;
	
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
}
