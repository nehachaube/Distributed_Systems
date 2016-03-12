package system;

import java.util.concurrent.TimeUnit;

/**
 * Catalog stores parameters about the whole system.
 */
public class Catalog {

    /** Settings for gossip group membership service */

    /** T_cleanup in milliseconds */
    public static final long CLEANUP_TIME = 5000;
    /** T_fail in milliseconds */
    public static final long FAIL_TIME = 2000;
    /** gap between gossiping membership list, measured in milliseconds */
    public static final long GOSSIP_GAP = 150;
    /** gap between negotiating with introducer, measured in milliseconds */
    public static final long INTRODUCER_NEGOTIATE_GAP = 2000;
    /** specify the time unit */
    public static final TimeUnit TIME_UNIT = TimeUnit.MILLISECONDS;
    /** specify the max packet bytes required for receiving membership list */
    public static final int MAX_UDP_PACKET_BYTES = 5000;
    /** specify the number of leave messages to gossip when voluntarily leave */
    public static final int NUM_LEAVE_GOSSIP = 1;
    /** specify the log for membership service */
    public static final String MEMBERSHIP_SERVICE_LOG = "ms.log";
    /** How long it takes for a new node to join a group */
    public static final long MEMBER_JOIN_TIME = 3000;

    /** Settings for log query service */

    /** specify the port number on which log query service is running */
    public static final int LOG_QUERY_SERVICE_PORT = 60001;

    /** Settings for remote grep client */
    public static final String[] HOST_LIST = new String[] { "fa15-cs425-g13-01.cs.illinois.edu",
            "fa15-cs425-g13-02.cs.illinois.edu", "fa15-cs425-g13-03.cs.illinois.edu",
            "fa15-cs425-g13-04.cs.illinois.edu", "fa15-cs425-g13-05.cs.illinois.edu",
            "fa15-cs425-g13-06.cs.illinois.edu", "fa15-cs425-g13-07.cs.illinois.edu" };

    /** Settings for Simple Distributed File System */

    /** specify the replication factor for SDFS */
    public static final int REPLICATION_FACTOR = 3;
    /** specify the port number on which membership service is running */
    public static final int SDFS_MEMBERSHIP_SERVICE_PORT = 60002;
    /** specify the port number of name node service */
    public static final int SDFS_NAMENODE_PORT = 60003;
    /** specify the port number of data node service */
    public static final int SDFS_DATANODE_PORT = 60004;
    /** specify the address of introducer */
    public static final String INTRODUCER_ADDRESS = "fa15-cs425-g13-04.cs.illinois.edu";
    /** specify the location where data node stores files */
    public static final String SDFS_DIR = "sdfs/";
    /** specify the log for SDFS */
    public static final String SDFS_LOG = "sdfs.log";
    /**
     * the delay for data node to send block report to a new elected name node
     */
    public static final long BLOCKREPORT_DELAY = 5000;
    /**
     * On startup, the NameNode enters a special state called Safemode.
     * Replication of data blocks does not occur when the NameNode is in the
     * Safemode state.
     */
    public static final long SAFE_MODE_DURATION = 20000;
    /** the period between successive replication check */
    public static final long REPLICATION_CHECK_PERIOD = 20000;
    /**
     * replication check will not touch recently added files until the period
     * passes
     */
    public static final long REPLICATION_SILENCE_PERIOD = 20000;

    /** Settings for Crane */
    public static final String NIMBUS_ADDRESS = "fa15-cs425-g13-04.cs.illinois.edu";
    /** specify the port number on which membership service is running */
    public static final int CRANE_MEMBERSHIP_SERVICE_PORT = 60005;
    /** specify the port number on which Supervisor service is running */
    public static final int SUPERVISOR_PORT = 60006;
    /** specify the port number on which Nimbus service is running */
    public static final int NIMBUS_PORT = 60007;
    /** specify the port number on which acker is listening */
    public static final int ACKER_PORT = 60008;
    /** ports starting from the number are reserved for worker threads */
    public static final int WORKER_PORT_RANGE = 60009;
    /** specify the log for nimbus */
    public static final String NIMBUS_LOG = "nb.log";
    /** specify the log for supervisor */
    public static final String SUPERVISOR_LOG = "sv.log";
    /** specify the log for acker */
    public static final String ACKER_LOG = "ack.log";
    /** specify the directory for Crane input */
    public static final String CRANE_DIR = "crane/";
    /** specify the time gap to check tuple timeout */
    public static final long TIMEOUT_CHECK_GAP = 500;
    /** how long will a tuple timeout */
    public static final long TUPLE_TIMEOUT = 20000;
    /** specify the time gap for spout and bolts to check finishing status */
    public static final long FINISH_STATUS_CHECK_GAP = 100;
    /**
     * specify the UDP receiver buffer size. It is useful for buffering multiple
     * packets when packets arrive faster than are being received
     */
    public static final int UDP_RECEIVE_BUFFER_SIZE = 212992;
    /**
     * default time to sleep after sending a tuple (Measured in milliseconds).
     * Throttle the emitting speed turns out to be extremely important to
     * control packet loss rate.
     */
    public static final int DEFAULT_SENDING_GAP = 100;

    /** other global settings */

    /** specify the root directory for logs */
    public static final String LOG_DIR = "log/";
    /** specify the character encoding used by the whole system */
    public static final String ENCODING = "UTF-8";
}