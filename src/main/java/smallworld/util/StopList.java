package smallworld.util;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import com.google.common.io.CharStreams;

public enum StopList {
	INSTANCE;
	
	private Set<String> stopwords;
	private static final String path = "src/main/resources/stoplist.txt";
	
	private StopList() {
		try {
			stopwords = new HashSet<>();
			//for (String line : CharStreams.readLines(new InputStreamReader(getClass().getResourceAsStream(path)))) {
			for (String line : CharStreams.readLines(new FileReader(path))) {
					stopwords.add(line);
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	public boolean isStopword(String word) {
		return stopwords.contains(word);
	}
}
