package smallworld;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import libsvm.svm;
import libsvm.svm_model;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.neo4j.graphalgo.GraphAlgoFactory;
import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.PathExpanders;
import org.neo4j.graphdb.RelationshipType;

import smallworld.data.RelationshipTypes;
import smallworld.data.query.Query;
import smallworld.navigation.AbstractNavigation;
import smallworld.navigation.ConcurrentNavigationThread;
import smallworld.navigation.ConcurrentNavigationThread.NavigationCompleteListener;
import smallworld.navigation.PrioritizedDFSNavigation;
import smallworld.navigation.PrioritizedNavigation;
import smallworld.navigation.ShortestNavigation;
import smallworld.navigation.TrainingNavigation;
import smallworld.navigation.TraversalNavigation;
import smallworld.navigation.evaluator.ClassificationEvaluator;
import smallworld.navigation.evaluator.DBLPInterestEvaluator;
import smallworld.navigation.evaluator.DegreeEvaluator;
import smallworld.navigation.evaluator.Evaluator;
import smallworld.navigation.evaluator.KleinbergEvaluator;
import smallworld.navigation.evaluator.LibSVMEvaluator;
import smallworld.navigation.evaluator.MinCommonCircleEvaluator;
import smallworld.navigation.evaluator.MostCommonCircleEvaluator;
import smallworld.navigation.evaluator.MostCommonFeatureEvaluator;
import smallworld.navigation.feature.DistanceMeasure;
import smallworld.navigation.feature.FeatureBuilder;
import smallworld.util.LibSVMUtils;
import smallworld.util.Pair;
import weka.classifiers.Classifier;
import weka.classifiers.bayes.NaiveBayes;
import weka.classifiers.functions.Logistic;
import weka.classifiers.trees.RandomForest;
import weka.core.converters.ArffSaver;

public class ConcurrentMain implements NavigationCompleteListener {
	
	private static final Logger logger = LogManager.getLogger();

	// For statistics
	final AtomicInteger numberOfPairsNavigated = new AtomicInteger();
	final AtomicInteger numberOfPairsPathFound = new AtomicInteger();
	final AtomicLong totalPathLength = new AtomicLong();
	final AtomicLong totalNumberOfNodesExplored = new AtomicLong();
	final int numberOfPairsToBeNavigated;
	
	// The serial numbers of pairs that already exist in the log file
	private Set<Integer> pairsVisited;

	// time
	final AtomicReference<Calendar> calendar = new AtomicReference<Calendar>();
	
	// Set the number of threads to the number of processors
	private static final int NUMBER_OF_THREADS = Runtime.getRuntime().availableProcessors();
	
	/**
	 * Carry out an experiment, given the type of navigation, the path to the neo4j graph.
	 * Need to specify random seeds for randomly selecting source and sink pairs
	 * 
	 * @param nav navigation strategy
	 * @param neo4jPath path to neo4j graph
	 * @param numberOfPairs how many number of pairs to experiment
	 * @param sourceSeed random seed for generating sources
	 * @param sinkSeed random seed for generating sinks
	 * @param print PrintStream for logging
	 * @throws IOException 
	 */
	public ConcurrentMain(PathFinder<Path> nav, String neo4jPath, int numberOfPairs, int randomSeed, String log) throws IOException {
		Query q = new Query(neo4jPath);
		//Query q = Query.getInstance();
		
		long time = System.currentTimeMillis();
		calendar.set(Calendar.getInstance());
		
		// sorted node id
		List<Long> nodeIds = Arrays.asList(q.cypherGetAllNodes());
		System.out.println("number of nodes: "+ nodeIds.size());
		
		ExecutorService executor = Executors.newFixedThreadPool(NUMBER_OF_THREADS);
		//List<Future<Path>> list = new ArrayList<Future<Path>>();

		// if no numberOfPairs specified, do all the pairs
		if (numberOfPairs == -1) {
			numberOfPairsToBeNavigated = nodeIds.size() * (nodeIds.size() - 1);
			numberOfPairs = numberOfPairsToBeNavigated;
		} else numberOfPairsToBeNavigated = numberOfPairs;// + numberOfPairsSkipped; // not checking if total more than total pairs
		
		// get pairs that already exist in the log file
		checkVisitedPairsInLog(log);
		
		// compose
		FileWriter writer = new FileWriter(log, true);
		
		List<Pair<Long, Long>> pairs = generateListOfPairs(nodeIds, numberOfPairsToBeNavigated, new Random(randomSeed));
		System.out.println("number of pairs: " + pairs.size());
						
		for (int i = 0; i < numberOfPairsToBeNavigated; i++) {
			
			// skip pairs that are already in log
			if (pairsVisited.contains(i+1)) {
				continue;
			}
			
			Pair<Long, Long> pair = pairs.get(i);
			
			Node n1 = q.cypherGetNode(pair.getFirst());
			Node n2 = q.cypherGetNode(pair.getSecond());
			
			ConcurrentNavigationThread worker = new ConcurrentNavigationThread(q.getGraphDatabaseService(), i+1, nav, n1, n2, writer);
			worker.addListener(this);
			
			Future<Path> submit = executor.submit(worker);
			//list.add(submit);
		}
		
		// This will make the executor accept no new threads
		// and finish all existing threads in the queue
		executor.shutdown();
		// Wait until all threads are finish
		while (!executor.isTerminated()) {}

		System.out.println("[ConcurrentMain] TIME TAKEN: " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - time));
		System.out.println(String.format("[ConcurrentMain] TOTAL PAIRS: %d (%.4f)",  numberOfPairsPathFound.get(), ((double)numberOfPairsPathFound.get()/numberOfPairs)));
		System.out.println(String.format("[ConcurrentMain] AVERAGE PATH LENGTH: %.4f", (totalPathLength.get() / (double) numberOfPairsPathFound.get())));
		//logger.log(Level.INFO, "TIME TAKEN: " + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - time));
		//logger.log(Level.INFO, String.format("TOTAL PAIRS: %d (%.4f)",  numberOfPairsPathFound.get(), ((double)numberOfPairsPathFound.get()/numberOfPairs)));
		//logger.log(Level.INFO, String.format("AVERAGE PATH LENGTH: %.4f", (totalPathLength.get() / (double) numberOfPairsPathFound.get())));
		System.out.printf("[ConcurrentMain] == AVERAGE VISITED NODES: %.4f ==\n", ((double) totalNumberOfNodesExplored.get() / (double) numberOfPairsPathFound.get()));
		//System.out.printf("[ConcurrentMain] == TRUE POSITIVE: %.4f ==\n", AbstractNavigation.getEvaluationResult());
		
		q.shutdown();
	}
	
	private boolean updateBetweennessCentrality = false;
	private static final String BETWEENNESS_CENTRALITY = "betweenness_centrality";
	@Override
	public void notifyOfThreadComplete(ConcurrentNavigationThread thread) {
		Path p = thread.getPath();
	
		if (updateBetweennessCentrality) {
			updateBetweennessCentrality(p);
		}
	
		int count = numberOfPairsNavigated.incrementAndGet();
		if (p != null) {
			int numberOfPathsFound = numberOfPairsPathFound.incrementAndGet();
			long length = totalPathLength.addAndGet(p.length());
			//@SuppressWarnings("deprecation")
			long nodes = totalNumberOfNodesExplored.addAndGet(thread.getNumberOfNodesExplored());
			
			// Display progress every 10 minutes
			Calendar lastUpdated = Calendar.getInstance();
			lastUpdated.setTime(calendar.get().getTime());
			Calendar now = Calendar.getInstance();
			lastUpdated.add(Calendar.MINUTE, 10);
			
			if (now.after(lastUpdated)) {
				calendar.set(now);
				//logger.log(Level.INFO, String.format("[ConcurrentMain] PROGRESS %d/%d\tLength: %.4f\tNodes: %.4f\t[%s]", count, numberOfPairsToBeNavigated, ((double)length)/count, ((double)nodes)/count, new Date().toString()));
				System.out.printf("[ConcurrentMain] PROGRESS %d/%d\tLength: %.4f\tNodes: %.4f\t[%s]\n", count, numberOfPairsToBeNavigated, ((double)length)/numberOfPathsFound, ((double)nodes)/numberOfPathsFound, new Date().toString());
			}
		}
		
		thread.removeListener(this);
	}

	private void updateBetweennessCentrality(Path p) {
		if (p == null || p.length() <= 1) return;
		for (Node n : p.nodes()) {
			if (n.getId() != p.startNode().getId() && n.getId() != p.endNode().getId()) {
				n.setProperty(BETWEENNESS_CENTRALITY, n.getProperty(BETWEENNESS_CENTRALITY, 0));
			}
		}
	}

	private void checkVisitedPairsInLog(String file) {
		
		final int PAIR_NUMBER = 0;
		//final int SOURCE = 1;
		//final int SINK = 2;
		final int PATH_LENGH = 3;
		final int NUMBER_OF_NODES_VISITED = 4;
		
		pairsVisited = new HashSet<Integer>();
		//BufferedReader reader = null;
		
		try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
			for (String line = reader.readLine(); line != null; line = reader.readLine()) {
				if (line.trim().length() > 0) {
					String[] tokens = line.split(",");
					pairsVisited.add(Integer.parseInt(tokens[PAIR_NUMBER]));
					numberOfPairsNavigated.incrementAndGet();
					if (tokens.length >= 4) {
						// a path is found
						numberOfPairsPathFound.incrementAndGet();
						totalPathLength.addAndGet(Long.parseLong(tokens[PATH_LENGH]));
						totalNumberOfNodesExplored.addAndGet(Long.parseLong(tokens[NUMBER_OF_NODES_VISITED]));
					}
				}
			}
		} catch (FileNotFoundException ignored) {
		} catch (IOException e) {
			logger.error("Error reading log file: " + file);
			e.printStackTrace();
		} 
		
		System.out.println("[ConcurrentMain] number of pairs exists in log: " + pairsVisited.size());
	}
	
	private static void usage() {
		System.err.println(
				new StringBuilder("smallworld.ConcurrentMain ")
					.append("NavigationApproach[Local|Global|Shortest|BiShortest|Training|Traversal] ")
					.append("Datapath [facebook|gplus|twitter|youtube|amazon|dblp|dblp-inproceedings|msacademy-circlesize|simple]")
					.append("Evaluator[Feature|Circle|MinCircle|Kleinberg|Degree|Logistic|RandomForest] ")
					.append("NumberOfPairs ")
					.toString());
	}
	
	private List<Pair<Long, Long>> generateListOfPairs(List<Long> nodeIds, int numberOfPairs, Random rand) {
		
		System.out.print("[ConcurrentMain] Generating source-sink pairs...");
		long startTime = System.currentTimeMillis();
		
		List<Pair<Long, Long>> pairs = new ArrayList<Pair<Long, Long>>(numberOfPairs);
		Set<Integer> set = new HashSet<Integer>();
		
		// if too many pairs of nodes (cannot put in an integer), then use max value of integer
		int max = nodeIds.size() * nodeIds.size();
		if (max <= 0) max = Integer.MAX_VALUE;
		
		while (set.size() < numberOfPairs) {
			int r = rand.nextInt(max);
			int source = r % nodeIds.size();
			int sink = r / nodeIds.size();
			if (source != sink && !set.contains(r)) {
				set.add(r);
				pairs.add(new Pair<Long, Long>(nodeIds.get(source), nodeIds.get(sink)));
			} 
		}
		
		System.out.println("done (" + DurationFormatUtils.formatDurationHMS(System.currentTimeMillis() - startTime) + ")");
		
		return pairs;
	}
	
	// For parsing command line arguments
	private static final int NAVIGATION = 0;
	private static final int NEO4J_PATH = 1;
	private static final int EVALUATOR = 2;
	private static final int NUMBER_OF_PAIRS = 3;
	
	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		
		System.out.println("[ConcurrentMain] number of processors: " + Runtime.getRuntime().availableProcessors());
		
		if (args.length != 4) {
			System.out.println("[ConcurrentMain] number of arguments: " + args.length + " (expected: 4)");
			usage();
			System.exit(0);
		} else {
			
			// Neo4J path for the graph
			String path = "neo4j" + File.separator + args[NEO4J_PATH];
			Constants.NEO4J_PATH = path;
			
			// Relation type
			RelationshipType type = null;
			// Direction
			Direction dir = null;
						
			switch (args[NEO4J_PATH]) {
			case "msacademy-circlesize":
			case "msacademy":
			case "facebook":
			case "facebook-exp":
			case "dblp":
			case "dblp-exp":
			case "dblp-small-exp":
			case "dblp-inproceedings":
			case "amazon":
			case "youtube":
			case "simple":
				type = RelationshipTypes.FRIEND.type();
				dir = Direction.BOTH;
				break;
			case "gplus":
			case "twitter":
				type = RelationshipTypes.FRIEND.type();
				dir = Direction.OUTGOING;
				break;
			}
			
			// Distance measure
			System.out.println("[ConcurrentMain] initialize shortest distance cache...");
			long startTime = System.currentTimeMillis();
			DistanceMeasure distanceFeature = new DistanceMeasure(args[NEO4J_PATH], type, dir);
			AbstractNavigation.setDistanceMeasure(distanceFeature);
			System.out.println("[ConcurrentMain] cache is initialized in " + ((System.currentTimeMillis() - startTime) / 1000d) + " secs");
			
			// Compose features
			FeatureBuilder features = new FeatureBuilder()
				.addFeature(FeatureBuilder.getCommonCirclesWithParent(args[NEO4J_PATH]))
				.addFeature(FeatureBuilder.getCommonCirclesWithTarget(args[NEO4J_PATH]))
				.addFeature(FeatureBuilder.getCommonCirclesBetweenParentTarget(args[NEO4J_PATH]))
				/*
				.addFeature(FeatureBuilder.hasCommonCirclesWithParent(args[NEO4J_PATH]))
				.addFeature(FeatureBuilder.hasCommonCirclesWithTarget(args[NEO4J_PATH]))
				.addFeature(FeatureBuilder.hasCommonCirclesBetweenParentTarget(args[NEO4J_PATH]))
				*/
				.addFeature(FeatureBuilder.getMinCommonCircleWithTarget(args[NEO4J_PATH]))
				.addFeature(FeatureBuilder.getMaxCommonCircleWithTarget(args[NEO4J_PATH]))
				.addFeature(FeatureBuilder.getEndNodeDegreeFeature(args[NEO4J_PATH], type, dir))
				.addFeature(FeatureBuilder.getParentNodeDegreeFeature(args[NEO4J_PATH], type, dir))
				.addFeature(FeatureBuilder.getPathLengthFeature(args[NEO4J_PATH]))
				.addClassFeature(distanceFeature);
	
			// Initialize evaluators
			Evaluator<Integer> evaluator = null;
			if (args[EVALUATOR].equals("Kleinberg")) evaluator = new KleinbergEvaluator(type, dir);
			else if (args[EVALUATOR].equals("Feature")) evaluator = new MostCommonFeatureEvaluator();
			else if (args[EVALUATOR].equals("Interests")) evaluator = new DBLPInterestEvaluator();
			else if (args[EVALUATOR].equals("Degree")) evaluator = new DegreeEvaluator(type, dir);
			else if (args[EVALUATOR].equals("Circle")) evaluator = new MostCommonCircleEvaluator();
			else if (args[EVALUATOR].equals("MinCircle")) evaluator = new MinCommonCircleEvaluator();
			else if (args[EVALUATOR].equals("Logistic")) {
				//Classifier classifier = new LibLINEAR(); classifier.setOptions(Utils.splitOptions("-S 0 -Z -D")); String log = "liblinear";
				//Classifier classifier = new WLSVM(); classifier.setOptions(Utils.splitOptions("-S 0 -K 2 -Z 1 -c 128 -g 2")); String log = "svm";
				//Classifier classifier = new J48(); String log = "j48";
				Classifier classifier = new Logistic(); //String log = "logistic";
				//Classifier classifier = new SimpleLogistic(); String log = "simplelogistic";
						
				try {
					evaluator = new ClassificationEvaluator(
									classifier, features, args[1] + ".arff");
					
					((ClassificationEvaluator) evaluator).printParameters();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			} else if (args[EVALUATOR].equals("NaiveBayes")) {
				Classifier classifier = new NaiveBayes();
						
				try {
					evaluator = new ClassificationEvaluator(
									classifier, features, args[1] + ".arff");
					
					((ClassificationEvaluator) evaluator).printParameters();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			} else if (args[EVALUATOR].equals("RandomForest")) {
				Classifier classifier = new RandomForest();
						
				try {
					evaluator = new ClassificationEvaluator(
									classifier, features, args[1] + ".arff");
					
					((ClassificationEvaluator) evaluator).printParameters();
				} catch (Exception e) {
					e.printStackTrace();
				}
				
			} else if (args[EVALUATOR].equals("LibSVM")) {
				try {
					svm_model model = svm.svm_load_model(args[NEO4J_PATH] + ".libsvm.model");
					evaluator = new LibSVMEvaluator(model, features);
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else throw new AssertionError("No such evaluator: " + args[EVALUATOR]);
			
			// Number of pairs need to be explored (exclude skipped ones)
			int numberOfPairs = Integer.parseInt(args[NUMBER_OF_PAIRS]);
			
			// Compose output filename
			String output = new StringBuilder(args[NEO4J_PATH]).append(".")
				.append(args[NAVIGATION]).append(".")
				.append(args[EVALUATOR]).append(".")
				.append(args[NUMBER_OF_PAIRS]).append(".")
				.append(Constants.PRORITY_QUEUE_MAX_SIZE).append(".")
				.append("log").toString();
			
			// Random seeds for generating pairs
			int randomSeed = 0;
			
			// Initialize navigation strategy
			PathFinder<Path> finder = null;
			if (args[NAVIGATION].equals("Local")) {
				finder = new PrioritizedDFSNavigation(
						PathExpanders.forTypeAndDirection(type, dir),
							evaluator);
			} else if (args[NAVIGATION].equals("Global")) {
				finder = new PrioritizedNavigation(
						PathExpanders.forTypeAndDirection(type, dir),
						evaluator);
			} else if (args[NAVIGATION].equals("BiShortest")) {
				finder = GraphAlgoFactory.shortestPath(
						PathExpanders.forTypeAndDirection(type, dir),
						Constants.LIMIT_OF_DEPTH); 
			} else if (args[NAVIGATION].equals("Shortest")) {
				finder = new ShortestNavigation(PathExpanders.forTypeAndDirection(type, dir));
			} else if (args[NAVIGATION].equals("Traversal")) {
				finder = new TraversalNavigation(PathExpanders.forTypeAndDirection(type, dir), evaluator);
			} else if (args[NAVIGATION].equals("Training")) {
				
				finder = new TrainingNavigation(
						PathExpanders.forTypeAndDirection(type, dir),
						features);
				
				// Use different seeds for training
				randomSeed = 1;
				
			} else {
				throw new IllegalArgumentException("No such navigation approach: " + args[NAVIGATION]);
			}
	
			System.out.println("[ConcurrentMain] log file: " + output);
			
			new ConcurrentMain(finder, path, numberOfPairs, randomSeed, output);
			
			// For saving ARFF files
			if (args[NAVIGATION].equals("Training")) {
				try {
					ArffSaver saver = new ArffSaver();
					//saver.setFile(new File(args[NEO4J_PATH] + "-" + args[NUMBER_OF_PAIRS] + ".arff"));
					saver.setFile(new File(args[NEO4J_PATH] + ".arff"));
					saver.setInstances(features.getInstances());
					saver.writeBatch();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				LibSVMUtils.arffToLibSVM(
						args[NEO4J_PATH] + ".arff", 
						args[NEO4J_PATH] + ".libsvm");
				LibSVMUtils.train(args[NEO4J_PATH] + ".libsvm");
			}
		}
		
	}

}
