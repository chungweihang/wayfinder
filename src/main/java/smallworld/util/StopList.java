package smallworld.util;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

import com.google.common.io.CharStreams;

public enum StopList {
	INSTANCE;
	
	private Set<String> stopwords;
	private static final String path = "stoplist.txt";
	
	private StopList() {
		try {
			stopwords = new HashSet<>();
			//for (String line : CharStreams.readLines(new InputStreamReader(getClass().getResourceAsStream(path)))) {
			for (String line : CharStreams.readLines(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(path)))) {
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
