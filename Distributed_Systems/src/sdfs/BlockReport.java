package sdfs;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.List;

/**
 * Block report is sent from data node to name node. It contains
 * information about all the SDFS files on that data node.
 */
public class BlockReport implements Serializable {

    private static final long serialVersionUID = 1L;

    private final InetAddress IPAddress;
    private final Datanode datanode;
    private final List<String> files;

    public BlockReport(InetAddress IPAddress, Datanode datanode, List<String> files) {
        this.IPAddress = IPAddress;
        this.datanode = datanode;
        this.files = files;
    }

    public InetAddress getIPAddress() {
        return this.IPAddress;
    }

    public Datanode getDatanode() {
        return this.datanode;
    }

    public List<String> getFiles() {
        return this.files;
    }
}