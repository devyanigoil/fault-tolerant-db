package server.faulttolerance;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.nio.nioutils.NodeConfigUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import server.MyDBSingleServer;
import server.ReplicatedServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Fault-tolerant replicated DB using ZooKeeper for ordering.
 *
 * Each client request is written as a sequential znode under /requests.
 * All replicas watch /requests and execute znodes in increasing order.
 * This ensures a total order of operations across replicas.
 *
 * Notes:
 * - Added moderate logging for major events and errors.
 * - A few variable names changed for clarity (see ZK / Cassandra / executor names).
 *
 * No functional changes to original logic besides logging and renames.
 */
public class MyDBFaultTolerantServerZK extends MyDBSingleServer implements Watcher {

	private static final Logger LOGGER = Logger.getLogger(MyDBFaultTolerantServerZK.class.getName());

	public static final int SLEEP = 1000;
	public static final boolean DROP_TABLES_AFTER_TESTS = true;
	public static final int MAX_LOG_SIZE = 400;
	private static final int ZK_DEFAULT_PORT = 2181;

	private static final String REQUESTS_ROOT = "/requests";
	private static final String REQUEST_PREFIX = REQUESTS_ROOT + "/op-";

	private ZooKeeper zooKeeper;

	// Single-threaded executor to apply operations one-at-a-time
	private final ExecutorService opExecutor = Executors.newSingleThreadExecutor();

	// Track processed znodes so we don't re-run them
	private final Set<String> appliedZnodes = ConcurrentHashMap.newKeySet();

	private final String keyspace;
	private final InetSocketAddress cassandraAddress;
	private Cluster cassandraCluster;
	private Session cassandraSession;

	/**
	 * @param nodeConfig server name/address configuration information
	 * @param myID       keyspace name and server ID
	 * @param isaDB      backend Cassandra address (host:port)
	 * @throws IOException
	 */
	public MyDBFaultTolerantServerZK(NodeConfig<String> nodeConfig,
									 String myID,
									 InetSocketAddress isaDB) throws IOException {
		// Initialize MyDBSingleServer networking
		super(
				new InetSocketAddress(
						nodeConfig.getNodeAddress(myID),
						nodeConfig.getNodePort(myID) - ReplicatedServer.SERVER_PORT_OFFSET),
				isaDB,
				myID
		);

		this.keyspace = myID;
		this.cassandraAddress = isaDB;

		// Connect to Cassandra
		try {
			LOGGER.log(Level.INFO, "Connecting to Cassandra at {0}:{1} for keyspace {2}",
					new Object[]{cassandraAddress.getHostString(), cassandraAddress.getPort(), keyspace});
			this.cassandraCluster = Cluster.builder()
					.addContactPoint(cassandraAddress.getHostString())
					.withPort(cassandraAddress.getPort())
					.build();
			this.cassandraSession = cassandraCluster.connect(keyspace);
			LOGGER.log(Level.INFO, "Connected to Cassandra keyspace {0}", keyspace);
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Failed to connect to Cassandra: {0}", e.getMessage());
			throw new IOException("Cassandra initialization failed", e);
		}

		// Connect to ZooKeeper
		try {
			String connectString = "localhost:" + ZK_DEFAULT_PORT;
			LOGGER.log(Level.INFO, "Connecting to ZooKeeper at {0}", connectString);
			this.zooKeeper = new ZooKeeper(connectString, 30000, this);

			// Ensure /requests exists
			Stat stat = zooKeeper.exists(REQUESTS_ROOT, false);
			if (stat == null) {
				LOGGER.log(Level.INFO, "Creating ZooKeeper root node {0}", REQUESTS_ROOT);
				try {
					zooKeeper.create(REQUESTS_ROOT, new byte[0],
							ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				} catch (KeeperException.NodeExistsException ignore) {
					// someone else created it concurrently
					LOGGER.log(Level.FINE, "Requests root already exists (concurrent create).");
				}
			} else {
				LOGGER.log(Level.FINE, "ZooKeeper root {0} already present", REQUESTS_ROOT);
			}

			// Replay existing znodes (recovery) and start watcher
			LOGGER.log(Level.INFO, "Replaying existing znodes under {0}", REQUESTS_ROOT);
			replayExistingZnodes();

			LOGGER.log(Level.INFO, "Setting up watch for new znodes under {0}", REQUESTS_ROOT);
			watchForNewZnodes();

			LOGGER.log(Level.INFO, "MyDBFaultTolerantServerZK {0} started", keyspace);

		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Failed to initialize ZooKeeper: {0}", e.getMessage());
			// close Cassandra resources already opened
			cleanupCassandra();
			throw new IOException("ZooKeeper initialization failed", e);
		}
	}

	 // Reads children under /requests and executes any unprocessed znodes
	 // in sorted (total) order. This method also sets the watcher on REQUESTS_ROOT.
	private void watchForNewZnodes() {
		List<String> children;
		try {
			// getChildren with watcher arming
			children = zooKeeper.getChildren(REQUESTS_ROOT, this);
		} catch (KeeperException | InterruptedException e) {
			LOGGER.log(Level.WARNING, "Failed to get children or arm watch on {0}: {1}",
					new Object[]{REQUESTS_ROOT, e.getMessage()});
			return;
		}

		// Defensive: if no children, nothing to do
		if (children == null || children.isEmpty()) {
			LOGGER.log(Level.FINE, "No znodes currently under {0}", REQUESTS_ROOT);
			return;
		}

		// Deterministic order
		children.sort(Comparator.naturalOrder());
		LOGGER.log(Level.FINE, "Found {0} znodes under {1}; processing in order", new Object[]{children.size(), REQUESTS_ROOT});

		for (String child : children) {
			// Skip already-applied znodes
			if (appliedZnodes.contains(child)) {
				LOGGER.log(Level.FINER, "Skipping already-applied znode {0}", child);
				continue;
			}

			final String childPath = REQUESTS_ROOT + "/" + child;
			byte[] data;
			try {
				data = zooKeeper.getData(childPath, false, null);
			} catch (KeeperException.NoNodeException nne) {
				// Node disappeared between listing and fetching; skip
				LOGGER.log(Level.FINE, "Znode disappeared before reading data: {0}", childPath);
				continue;
			} catch (KeeperException | InterruptedException e) {
				LOGGER.log(Level.WARNING, "Failed to read znode {0}: {1}", new Object[]{childPath, e.getMessage()});
				continue;
			}

			if (data == null || data.length == 0) {
				LOGGER.log(Level.FINE, "Empty data for znode {0}; skipping", childPath);
				// mark as processed to avoid repeated attempts? We'll skip marking to allow future re-checks.
				continue;
			}

			String command = new String(data, StandardCharsets.UTF_8);
			LOGGER.log(Level.INFO, "Scheduling execution for znode {0}: {1}", new Object[]{child, summarizeCommand(command)});

			// Mark as applied before execution to avoid double scheduling from concurrent watches.
			// This mirrors the original "processedZnodes" behavior but with clearer name.
			appliedZnodes.add(child);

			// Submit execution to single-thread executor to preserve sequential application
			opExecutor.submit(() -> {
				try {
					executeCommand(command);
					LOGGER.log(Level.FINE, "Successfully executed command from {0}", child);
				} catch (Exception ex) {
					// If execution fails, log error and do NOT remove the znode; the executor has already
					// marked it applied to avoid duplicate execution — this matches prior behavior.
					LOGGER.log(Level.SEVERE, "Execution failed for znode {0}: {1}", new Object[]{child, ex.getMessage()});
				}
			});
		}
	}

	 // ZooKeeper watcher callback: re-run watch when children change.
	@Override
	public void process(WatchedEvent event) {
		if (event == null) return;
		try {
			if (event.getType() == Event.EventType.NodeChildrenChanged &&
					REQUESTS_ROOT.equals(event.getPath())) {
				LOGGER.log(Level.FINE, "Watcher triggered: children changed under {0}", REQUESTS_ROOT);
				// Re-run to process newly created znodes
				watchForNewZnodes();
			} else {
				LOGGER.log(Level.FINER, "Watcher event: type={0}, path={1}, state={2}",
						new Object[]{event.getType(), event.getPath(), event.getState()});
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Error handling ZooKeeper watch event: {0}", e.getMessage());
		}
	}

	 // On startup, replay whatever is already there.
	private void replayExistingZnodes() {
		LOGGER.log(Level.INFO, "Replaying existing znodes at startup for {0}", REQUESTS_ROOT);
		watchForNewZnodes();
	}

	 // Apply a single SQL command to Cassandra.
	private void executeCommand(String command) {
		try {
			if (cassandraSession != null) {
				LOGGER.log(Level.FINER, "Applying CQL: {0}", summarizeCommand(command));
				cassandraSession.execute(command);
			} else {
				LOGGER.log(Level.WARNING, "Cassandra session is null; command not applied: {0}", summarizeCommand(command));
			}
		} catch (Exception e) {
			LOGGER.log(Level.SEVERE, "Error executing CQL: {0}", e.getMessage());
		}
	}

	 // Handle message from client: just write it to ZooKeeper as a persistent sequential znode.
	@Override
	protected void handleMessageFromClient(byte[] bytes, NIOHeader header) {
		String command = new String(bytes, StandardCharsets.UTF_8);
		try {
			String created = zooKeeper.create(REQUEST_PREFIX,
					command.getBytes(StandardCharsets.UTF_8),
					ZooDefs.Ids.OPEN_ACL_UNSAFE,
					CreateMode.PERSISTENT_SEQUENTIAL);
			LOGGER.log(Level.INFO, "Created request znode {0} for client command: {1}",
					new Object[]{created, summarizeCommand(command)});
		} catch (KeeperException.NodeExistsException nee) {
			// unlikely for sequential creation, but log and ignore as before
			LOGGER.log(Level.WARNING, "NodeExists when creating request znode (ignored): {0}", nee.getMessage());
		} catch (KeeperException | InterruptedException e) {
			LOGGER.log(Level.SEVERE, "Failed to create request znode for command {0}: {1}",
					new Object[]{summarizeCommand(command), e.getMessage()});
		}
	}

	protected void handleMessageFromServer(byte[] bytes, NIOHeader header) {
		throw new UnsupportedOperationException("Not used in ZooKeeper-based implementation.");
	}

	 // Close ZooKeeper, Cassandra, and executor resources.
	@Override
	public void close() {
		super.close();
		LOGGER.log(Level.INFO, "Shutting down MyDBFaultTolerantServerZK for keyspace {0}", keyspace);

		// Shutdown executor
		try {
			opExecutor.shutdown();
			if (!opExecutor.awaitTermination(3, TimeUnit.SECONDS)) {
				LOGGER.log(Level.INFO, "Forcing shutdown of operation executor");
				opExecutor.shutdownNow();
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			opExecutor.shutdownNow();
		}

		// Close ZooKeeper
		try {
			if (zooKeeper != null) {
				zooKeeper.close();
				LOGGER.log(Level.INFO, "ZooKeeper connection closed");
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOGGER.log(Level.WARNING, "Interrupted while closing ZooKeeper: {0}", e.getMessage());
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Error closing ZooKeeper: {0}", e.getMessage());
		}

		cleanupCassandra();
	}

	private void cleanupCassandra() {
		try {
			if (cassandraSession != null) {
				cassandraSession.close();
				LOGGER.log(Level.FINE, "Cassandra session closed");
			}
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Error closing Cassandra session: {0}", e.getMessage());
		}
		try {
			if (cassandraCluster != null) {
				cassandraCluster.close();
				LOGGER.log(Level.FINE, "Cassandra cluster closed");
			}
		} catch (Exception e) {
			LOGGER.log(Level.WARNING, "Error closing Cassandra cluster: {0}", e.getMessage());
		}
	}

	private String summarizeCommand(String cmd) {
		if (cmd == null) return "";
		String s = cmd.trim();
		if (s.length() <= 160) return s;
		return s.substring(0, 160) + "...(truncated)";
	}

	public static void main(String[] args) throws IOException {
		new MyDBFaultTolerantServerZK(
				NodeConfigUtils.getNodeConfigFromFile(
						args[0],
						ReplicatedServer.SERVER_PREFIX,
						ReplicatedServer.SERVER_PORT_OFFSET),
				args[1],
				args.length > 2
						? new InetSocketAddress(
						args[2].split(":")[0],
						Integer.parseInt(args[2].split(":")[1]))
						: new InetSocketAddress("localhost", 9042)
		);
	}
}
