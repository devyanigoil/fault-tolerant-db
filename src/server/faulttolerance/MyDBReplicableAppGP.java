package server.faulttolerance;

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class should implement your {@link Replicable} database app if you wish
 * to use Gigapaxos.
 * <p>
 * Make sure that both a single instance of Cassandra is running at the default
 * port on localhost before testing.
 * <p>
 * Tips:
 * <p>
 * 1) No server-server communication is permitted or necessary as you are using
 * gigapaxos for all that.
 * <p>
 * 2) A {@link Replicable} must be agnostic to "myID" as it is a standalone
 * replication-agnostic application that via its {@link Replicable} interface is
 * being replicated by gigapaxos. However, in this assignment, we need myID as
 * each replica uses a different keyspaceName (because we are pretending different
 * replicas are like different keyspaceNames), so we use myID only for initiating
 * the connection to the backend data store.
 * <p>
 * 3) This class is never instantiated via a main method. You can have a main
 * method for your own testing purposes but it won't be invoked by any of
 * Grader's tests.
 */
public class MyDBReplicableAppGP implements Replicable {

    /**
     * Set this value to as small a value with which you can get tests to still
     * pass. The lower it is, the faster your implementation is. Grader* will
     * use this value provided it is no greater than its MAX_SLEEP limit.
     * Faster
     * is not necessarily better, so don't sweat speed. Focus on safety.
     */
    public static final int SLEEP = 1000;


    private Session session;
    private Cluster cluster;
    private final String keyspaceName;

    private static final Logger log = Logger.getLogger(MyDBReplicableAppGP.class.getName());
    public static final int MAX_LOG_SIZE = 400;

    private final Set<String> executedReqs;
    private final List<String> requestLog;

    private long lastCheckpointTime;
    private static final long CHECKPOINT_INTERVAL = 3000;

    private long latestExecutedSeq = 0;

    /**
     * All Gigapaxos apps must either support a no-args constructor or a
     * constructor taking a String[] as the only argument. Gigapaxos relies on
     * adherence to this policy in order to be able to reflectively construct
     * customer application instances.
     *
     * @param args Singleton array whose args[0] specifies the keyspaceName in the
     *             backend data store to which this server must connect.
     *             Optional args[1] and args[2]
     * @throws IOException
     */
    public MyDBReplicableAppGP(String[] args) {

        if (args == null || args.length == 0 || args[0] == null)
            throw new IllegalArgumentException("keyspaceName name (args[0]) required");
        this.keyspaceName = args[0];


        this.executedReqs = ConcurrentHashMap.newKeySet();
        this.requestLog = Collections.synchronizedList(new ArrayList<>());
        String host = "127.0.0.1";
        int port = 9042;
        try {
            // Establishing Cassandra Connection
            this.cluster = Cluster.builder().addContactPoint(host).withPort(port).build();

            this.session = cluster.connect(keyspaceName);

            //Creating keyspace if not exists
            session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspaceName +
                    " WITH replication = {'class':'SimpleStrategy','replication_factor':'1'}");

            //Creating table if not exists
            session.execute("CREATE TABLE IF NOT EXISTS grade (" +
                    "id int PRIMARY KEY," +
                    "events list<int>" +
                    ")");

        } catch (Exception e) {
            log.severe("Failed to initialize database connection: " + e.getMessage());
            e.printStackTrace();
        }
    }


    /**
     * Refer documentation of {@link Replicable#execute(Request, boolean)} to
     * understand what the boolean flag means.
     * <p>
     * You can assume that all requests will be of type {@link
     * edu.umass.cs.gigapaxos.paxospackets.RequestPacket}.
     *
     * @param request
     * @param b
     * @return
     */
    @Override
    public boolean execute(Request request, boolean b) {
        return execute(request);
    }

    /**
     * Refer documentation of
     * {@link edu.umass.cs.gigapaxos.interfaces.Application#execute(Request)}
     *
     * @param request
     * @return
     */
    @Override
    public boolean execute(Request request) {
        if (!(request instanceof RequestPacket)) {
            log.warning("Expected RequestPacket but got " + request.getClass());
            return false;
        }
        RequestPacket rp = (RequestPacket) request;
        String reqStr = new String(rp.getRequestValue().getBytes(), StandardCharsets.UTF_8);
        String requestId = String.valueOf(((RequestPacket) request).getRequestID());


        String sql = null;
        try {
            JSONObject j = new JSONObject(reqStr);
            if (j.has("QV")) sql = j.getString("QV");
        } catch (JSONException ignored) {
            sql = reqStr;
        }
        if (sql == null) sql = reqStr;

        try {
            String result = String.valueOf(executeSQL(sql));
            // Execute the SQL on Cassandra
            boolean success = executeSQL(sql);

            if (success) {
                executedReqs.add(requestId);

                synchronized (requestLog) {
                    //Add to Log
                    requestLog.add(requestId);
                    if (requestLog.size() > MAX_LOG_SIZE) {
                        String oldestId = requestLog.remove(0);
                        executedReqs.remove(oldestId);
                    }
                }

                // Update sequence number from SQL for checkpoint metadata
                LastSequence(sql);

                if (System.currentTimeMillis() - lastCheckpointTime > CHECKPOINT_INTERVAL) {
                    lastCheckpointTime = System.currentTimeMillis();
                }

                System.out.println("Executed request: " + requestId);
            }

            rp.setResponse(Arrays.toString(result.getBytes(StandardCharsets.UTF_8)));
            return true;
        } catch (Exception e) {
            log.severe("execute failed for sql: " + sql + " ; error: " + e.getMessage());
            rp.setResponse(Arrays.toString(("ERROR: " + e.getMessage()).getBytes(StandardCharsets.UTF_8)));
            return false;
        }
    }

    //Executing SQL command
    private boolean executeSQL(String sql) {
        try {
            session.execute("USE " + keyspaceName);
            session.execute(sql);
            session.execute("COMMIT");
            return true;

        } catch (InvalidQueryException e) {
            System.err.println("Invalid query: " + e.getMessage());
            try {
                session.execute("CREATE TABLE IF NOT EXISTS grade (" +
                        "id int PRIMARY KEY," +
                        "events list<int>" +
                        ")");
                session.execute("USE " + keyspaceName);
                session.execute(sql);
                session.execute("COMMIT");
                return true;
            } catch (Exception retryException) {
                System.err.println("Recovery failed: " + retryException.getMessage());
                return false;
            }

        } catch (Exception e) {
            System.err.println("SQL execution failed: " + e.getMessage());
            return false;
        }
    }


    //Update the last sequence number from SQL
    private void LastSequence(String sql) {
        if (sql.toUpperCase().contains("EVENTS+[")) {

            int start = sql.indexOf("[");
            int end = sql.indexOf("]");

            if (start != -1 && end != -1 && start < end) {
                String seqStr = sql.substring(start + 1, end).trim();

                try {
                    long seq = Long.parseLong(seqStr);
                    if (seq > latestExecutedSeq) {
                        latestExecutedSeq = seq;
                    }
                } catch (NumberFormatException e) {
                    log.warning("Error parsing sequence number: " + seqStr);
                }
            }
        }
    }


    //Reads all data from Cassandra to ensure complete table capture.
    private Map<Integer, List<Integer>> readAllTableData() {
        Map<Integer, List<Integer>> state = new HashMap<>();
        try {
            session.execute("USE " + keyspaceName);
            ResultSet resultSet = session.execute("SELECT id, events FROM grade");

            for (Row row : resultSet) {
                int id = row.getInt("id");
                List<Integer> events = new ArrayList<>(row.getList("events", Integer.class));
                state.put(id, events);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return state;
    }

    /**
     * Refer documentation of {@link Replicable#checkpoint(String)}.
     *
     * @param s
     * @return
     */
    @Override
    public String checkpoint(String s) {

        try {
            // Create JSON structure
            JSONObject checkpointJson = new JSONObject();

            // Adding metadata
            checkpointJson.put("version", 1);
            checkpointJson.put("keyspace", this.keyspaceName);
            checkpointJson.put("checkpointTime", System.currentTimeMillis());
            checkpointJson.put("latestSeq", this.latestExecutedSeq);

            // Adding executed requests as JSON array
            JSONArray executedArray = new JSONArray();
            for (String reqId : this.executedReqs) {
                executedArray.put(reqId);
            }
            checkpointJson.put("executedRequests", executedArray);

            // Adding database state
            Map<Integer, List<Integer>> dbState = readAllTableData();

            JSONObject dbJson = new JSONObject();
            for (Map.Entry<Integer, List<Integer>> entry : dbState.entrySet()) {
                JSONArray eventsArray = new JSONArray();
                for (Integer event : entry.getValue()) {
                    eventsArray.put(event);
                }
                dbJson.put(String.valueOf(entry.getKey()), eventsArray);
            }
            checkpointJson.put("databaseState", dbJson);

            //Converting to string
            String jsonString = checkpointJson.toString();

            log.info("Checkpoint created successfully:");
            log.info("  - Size: " + jsonString.length() + " characters");
            log.info("  - Executed requests: " + executedArray.length());
            log.info("  - Database rows: " + dbState.size());
            log.info("  - Latest sequence: " + latestExecutedSeq);
            return jsonString;

        } catch (Exception e) {
            return "{}";
        }
    }

    /**
     * Refer documentation of {@link Replicable#restore(String, String)}
     *
     * @param s
     * @param s1
     * @return
     */
    @Override
    public boolean restore(String s, String s1) {

        if (s1 == null || s1.isEmpty() || s1.equals("{}")) {
            log.info("Empty checkpoint");
            executedReqs.clear();
            requestLog.clear();
            return true;
        }

        try {
            // Parse JSON
            JSONObject checkpointJson = new JSONObject(s1);

            // Restore metadata
            this.latestExecutedSeq = checkpointJson.optLong("latestSeq", 0);
            this.lastCheckpointTime = checkpointJson.optLong("checkpointTime", System.currentTimeMillis());

            // Restore executed requests
            executedReqs.clear();
            requestLog.clear();
            JSONArray executedArray = checkpointJson.optJSONArray("executedRequests");
            if (executedArray != null) {
                for (int i = 0; i < executedArray.length(); i++) {
                    String reqId = executedArray.getString(i);
                    executedReqs.add(reqId);
                    requestLog.add(reqId);
                }
            }

            // Restore database state
            JSONObject dbJson = checkpointJson.optJSONObject("databaseState");
            if (dbJson != null) {
                restoreDatabaseStateFromJson(dbJson);
            }

            log.info("JSON checkpoint restored with " + executedReqs.size() + " requests");
            return true;

        } catch (Exception e) {
            log.severe("JSON restore failed: " + e.getMessage());
            return false;
        }
    }

    //Restoring database state from JSON representation.
    private void restoreDatabaseStateFromJson(JSONObject dbJson) {
        try {
            session.execute("USE " + keyspaceName);
            session.execute("TRUNCATE grade;");

            //Restoring each row from JSON
            Iterator<String> keys = dbJson.keys();
            while (keys.hasNext()) {
                String keyStr = keys.next();
                int id = Integer.parseInt(keyStr);
                JSONArray eventsArray = dbJson.getJSONArray(keyStr);

                StringBuilder sql = new StringBuilder();
                sql.append("INSERT INTO grade (id, events) VALUES (")
                        .append(id)
                        .append(", [");

                for (int i = 0; i < eventsArray.length(); i++) {
                    if (i > 0) sql.append(", ");
                    sql.append(eventsArray.getInt(i));
                }
                sql.append("]);");

                session.execute(sql.toString());
            }

            log.info("Restored " + dbJson.length() + " rows from JSON");

        } catch (Exception e) {
            log.severe("Failed to restore from JSON: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * No request types other than {@link edu.umass.cs.gigapaxos.paxospackets
     * .RequestPacket will be used by Grader, so you don't need to implement
     * this method.}
     *
     * @param s
     * @return
     * @throws RequestParseException
     */
    @Override
    public Request getRequest(String s) throws RequestParseException {
        return null;
    }

    /**
     * @return Return all integer packet types used by this application. For an
     * example of how to define your own IntegerPacketType enum, refer {@link
     * edu.umass.cs.reconfiguration.examples.AppRequest}. This method does not
     * need to be implemented because the assignment Grader will only use
     * {@link
     * edu.umass.cs.gigapaxos.paxospackets.RequestPacket} packets.
     */
    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>();
    }

    //Clean shutdown
    public void close() {
        try {
            if (session != null && !session.isClosed()) {
                session.close();
            }
            if (cluster != null && !cluster.isClosed()) {
                cluster.close();
            }
            System.out.println("Closed database connection for keyspaceName: " + keyspaceName);
        } catch (Exception e) {
            System.err.println("Error during close: " + e.getMessage());
        }
    }

}