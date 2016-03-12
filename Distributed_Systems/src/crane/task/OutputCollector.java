package crane.task;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

import crane.topology.Address;
import crane.topology.IComponent;
import crane.tuple.ITuple;
import system.CommonUtils;

public class OutputCollector {
    private final DatagramSocket sendSocket;
    private final Address ackerAddress;

    public OutputCollector(Address ackerAddress, DatagramSocket sendSocket) {
        this.ackerAddress = ackerAddress;
        this.sendSocket = sendSocket;
    }

    public OutputCollector(Address ackerAddress) throws SocketException {
        this(ackerAddress, new DatagramSocket());
    }

    public void ack(int tupleID, long checksum) throws IOException {
        AckMessage msg = new AckMessage(tupleID, checksum);
        byte[] bytes = msg.toBytes();
        DatagramPacket packet = new DatagramPacket(bytes, bytes.length, ackerAddress.IP,
                ackerAddress.port);
        sendSocket.send(packet);
    }

    public long emit(ITuple tuple, IComponent comp, long checksum) throws IOException {
        for (IComponent child : comp.getChildren()) {
            int taskNo = child.getPartitionStrategy().partition(tuple, child.getParallelism());
            tuple.setSalt();

            Address add = child.getTaskAddress(taskNo);
            CommonUtils.sendObjectOverUDP(tuple, add.IP, add.port, sendSocket);
            checksum ^= tuple.getSalt();
        }
        return checksum;
    }
}
