package smallworld;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.io.IOException;

public class LogAnalyzer {

	int numberOfPairs;
	int numberOfPathsFound;
	int numberOfLogs;
	double totalPathLengths;
	double totalNumberOfNodesVisited;
	double totalNumberOfEdgesVisited;
	double totalNumberOfEdgesVisitedShorteningPaths;
	
	public LogAnalyzer(String folder, final String prefix) {
		
		System.out.println("[LogAnalyzer] read file starts with " + prefix + " in " + folder);
		
		File dir = new File(folder);
		File[] files = dir.listFiles(new FilenameFilter() {

			@Override
			public boolean accept(File f, String name) {
				
				//System.out.println(f.getAbsolutePath() + ":" + name);
				
				if (name.startsWith(prefix)) {
					return true;
				}
				
				return false;
			}
			
		});
		
		for (File file : files) {
			System.out.print("[LogAnalyzer] reading " + file + "...");
			load(file);
			System.out.println("done");
		}
		
		System.out.println("[LogAnalyzer] total log files: " + numberOfLogs);
		System.out.println("[LogAnalyzer] total pairs: " + numberOfPairs);
		System.out.println("[LogAnalyzer] total paths found: " + numberOfPathsFound);
		System.out.println("[LogAnalyzer] average path length: " + totalPathLengths / numberOfPairs);
		System.out.println("[LogAnalyzer] average number of nodes visited: " + totalNumberOfNodesVisited / numberOfPairs);
		System.out.println("[LogAnalyzer] percentage of corrected edges visited: " + totalNumberOfEdgesVisited / totalNumberOfEdgesVisitedShorteningPaths);
	}
	
	private void load(File log) {
		BufferedReader reader = null;
		
		try {
			reader = new BufferedReader(new FileReader(log));
			numberOfLogs++;
			
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				line = line.trim();
				if (line.length() == 0) continue;
				
				numberOfPairs++;
				
				String[] tokens = line.split(",");
				if (tokens.length != 7) {
					continue;
				}
				
				numberOfPathsFound++;
				totalPathLengths += Integer.parseInt(tokens[3]); // path length
				totalNumberOfNodesVisited += Integer.parseInt(tokens[4]); // nodes visited
				totalNumberOfEdgesVisited += Integer.parseInt(tokens[5]); // edge visited
				totalNumberOfEdgesVisitedShorteningPaths += Integer.parseInt(tokens[6]); // edge visited that shorten paths
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != reader) try { reader.close(); } catch (IOException ignored) {}
		}
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		new LogAnalyzer("/Users/chang/Documents/research/small-worlds/log", "dblp.Local.FRIEND.BOTH.Kleinberg.10000.");
	}

}
