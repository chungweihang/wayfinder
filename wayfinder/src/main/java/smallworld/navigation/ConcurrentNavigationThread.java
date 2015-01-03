package smallworld.navigation;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArraySet;

import org.neo4j.graphalgo.PathFinder;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Path;
import org.neo4j.graphdb.Transaction;

import smallworld.navigation.AbstractNavigation.Metadata;

/**
 * A callable thread that search a path between a pair of nodes using a given navigation strategy.
 *  
 * @author chang
 *
 */
public class ConcurrentNavigationThread implements Callable<Path> {

	//private static AtomicInteger count = new AtomicInteger(0); 
	
	private final GraphDatabaseService graphDb;
	private final int serialNumber;
	private final PathFinder<Path> nav; 
	private final Node source, sink;
	private final FileWriter log;
	private Path path;
	
	public ConcurrentNavigationThread(GraphDatabaseService graphDb, int serial, PathFinder<Path> nav, Node source, Node sink, FileWriter log) {
		this.graphDb = graphDb;
		this.serialNumber = serial;
		this.nav = AbstractNavigation.copy(nav);
		this.source = source;
		this.sink = sink;
		this.log = log;
	}
	
	public int getSerialNumber() {
		return serialNumber;
	}

	//@Deprecated
	public long getNumberOfNodesExplored() {
		if (nav instanceof AbstractNavigation) {
			Metadata metadata = ((AbstractNavigation) nav).lastMetadata;
			return metadata.getTotalNodesExplored();
			//return ((AbstractNavigation) nav).numberOfVisitedNodes;
		}
		
		return 0;
	}
	
	@Override
	public Path call() {
		try (Transaction tx = graphDb.beginTx()) {
			path = nav.findSinglePath(source, sink);
			
			if (this.updateBetweennessCentrality) {
				updateBetweennessCentrality(path);
				tx.success();
			}
			
			try {
				if (path != null) {
					//print.println(path.length() + ", " + this.getNumberOfNodesExplored() + ", " + path);
					if (nav instanceof AbstractNavigation) {
						AbstractNavigation abstractNav = (AbstractNavigation) nav;
						log.write(new StringBuilder()
								.append(serialNumber).append(",")
								.append(source.getId()).append(",")
								.append(sink.getId()).append(",")
								.append(path.length()).append(",")
								.append(this.getNumberOfNodesExplored()).append(",")
								.append(abstractNav.getNumberOfVisitedNodesShorteningPaths()).append(",")
								.append(abstractNav.getNumberOfVisitedNodes()).append(System.getProperty("line.separator")).toString());
					} else {
						log.write(new StringBuilder()
						.append(serialNumber).append(",")
						.append(source.getId()).append(",")
						.append(sink.getId()).append(",")
						.append(path.length()).append(",")
						.append(this.getNumberOfNodesExplored()).append(System.getProperty("line.separator")).toString());
					}
				} else {
					log.write(new StringBuilder()
					.append(serialNumber).append(",")
					.append(source.getId()).append(",")
					.append(sink.getId()).append(System.getProperty("line.separator")).toString());
				}
				
				log.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			return path;
		} finally {
			notifyListeners();
		}
	}
	
	/*
	public int getNumberOfVisitedNodesShorteningPaths() {
		if (!(nav instanceof AbstractNavigation)) return 0;
		return ((AbstractNavigation) nav).getNumberOfVisitedNodesShorteningPaths();
	}
	
	public int getNumberOfVisitedNodes() {
		if (!(nav instanceof AbstractNavigation)) return 0;
		return ((AbstractNavigation) nav).getNumberOfVisitedNodes();
	}
	*/
	
	// Set if the found paths should be used to update betweenness centrality
	// Usually set to true when BiShortest is used
	private boolean updateBetweennessCentrality = true;
	private static final String betweennessCentralityPropertyName = "betweenness_centrality";
	private static void updateBetweennessCentrality(Path p) {
		//System.out.println(p);
		// start and end nodes are adjacent
		if (p == null || p.length() <= 1) return;
		
		for (Iterator<Node> it = p.nodes().iterator(); it.hasNext(); ) {
			Node n = it.next();
			
			if (n.getId() != p.startNode().getId() && n.getId() != p.endNode().getId()) {
				//System.out.print("updating node:" + n.getId() + " from " + n.getProperty(betweennessCentralityPropertyName, 0));
				n.setProperty(betweennessCentralityPropertyName, (int) n.getProperty(betweennessCentralityPropertyName, 0) + 1);
				//System.out.println(" to " + n.getProperty(betweennessCentralityPropertyName, 0));
				
			}
		}
	}
	
	public Path getPath() {
		return path;
	}

	// Thread-safe set
	private final Set<NavigationCompleteListener> listeners = new CopyOnWriteArraySet<NavigationCompleteListener>();
	
	public final void addListener(final NavigationCompleteListener listener) {
		listeners.add(listener);
	}
	
	public final void removeListener(final NavigationCompleteListener listener) {
		listeners.remove(listener);
	}
	
	private final void notifyListeners() {
		for (NavigationCompleteListener listener : listeners) {
			listener.notifyOfThreadComplete(this);
		}
	}

	/**
	 * An listener that is notified when a thread (callable) is completed
	 * 
	 * @author chang
	 *
	 */
	public interface NavigationCompleteListener {
		void notifyOfThreadComplete(final ConcurrentNavigationThread thread);
	}
}
