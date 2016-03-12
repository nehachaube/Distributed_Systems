package crane;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import crane.task.AckMessage;
import crane.task.CraneWorker;
import crane.task.Task;
import crane.topology.Address;
import system.Catalog;
import system.CommonUtils;

/**
 * Acker takes responsible for acking for tuples.
 */
public class Acker implements CraneWorker {

    // Assume spout never dies, otherwise there is no way to tell apart ack
    // message from previous round and current round
    private Address spoutAddress;
    private final DatagramSocket ds;
    private final Logger logger;
    private final Map<Integer, Long> tupleChecksums;

    public Acker(Address spoutAddress, int port, Logger logger) throws SocketException {
        this.spoutAddress = spoutAddress;
        this.ds = new DatagramSocket(port);
        this.ds.setReceiveBufferSize(Catalog.UDP_RECEIVE_BUFFER_SIZE);
        this.logger = logger;
        this.tupleChecksums = Collections.synchronizedMap(new HashMap<>());
    }

    public void setSpoutAddress(Address spoutAddress) {
        logger.info("Spout address reset to " + spoutAddress);
        this.spoutAddress = spoutAddress;
        tupleChecksums.clear();
    }

    @Override
    public void run() {
        DatagramPacket packet = new DatagramPacket(new byte[Catalog.MAX_UDP_PACKET_BYTES],
                Catalog.MAX_UDP_PACKET_BYTES);
        try {
            while (true) {
                ds.receive(packet);
                // Use the serialization written by myself to improve
                // performance
                AckMessage msg = new AckMessage(packet.getData());

                int tid = msg.tupleID;
                long checksum = msg.checksum;
                logger.info(String.format("Received checksum for tupleID %s: %s", tid, checksum));
                
                long cs = tupleChecksums.getOrDefault(tid, 0L);
                cs ^= checksum;
                logger.info(String.format("New checksum for tupleID %s: %s", tid, cs));

                if (cs == 0) {
                    tupleChecksums.remove(tid);
                    CommonUtils.sendObjectOverUDP(tid, spoutAddress.IP, spoutAddress.port, ds);
                } else {
                    tupleChecksums.put(tid, cs);
                }
            }
        } catch (IOException e) {
            logger.info("Acker terminated.");
        }
    }

    @Override
    public void setTask(Task task) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void terminate() {
        ds.close();
    }

}
