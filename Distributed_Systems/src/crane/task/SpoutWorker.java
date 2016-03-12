package crane.task;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import crane.INimbus;
import crane.spout.ISpout;
import crane.topology.Address;
import crane.tuple.ITuple;
import system.Catalog;

public class SpoutWorker implements CraneWorker {

    private class TupleStatus {
        ITuple tuple;
        long timestamp;

        TupleStatus(ITuple tuple, long timestamp) {
            this.tuple = tuple;
            this.timestamp = timestamp;
        }
    }

    private class AckReceiver implements Runnable {
        DatagramSocket ds;

        AckReceiver(DatagramSocket ds) throws SocketException {
            this.ds = ds;
        }

        @Override
        public void run() {
            DatagramPacket packet = new DatagramPacket(new byte[Catalog.MAX_UDP_PACKET_BYTES],
                    Catalog.MAX_UDP_PACKET_BYTES);
            try {
                while (true) {
                    ds.receive(packet);
                    ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                    int tupleId = (int) new ObjectInputStream(bais).readObject();
                    
                    logger.info(String.format("Received ack for tuple %s", tupleId));
                    
                    completedTuples.add(tupleId);
                }
            } catch (IOException | ClassNotFoundException e) {
                logger.info("Ack receiver thread terminated.");
            }
        }
    }

    private class TimeoutChecker implements Runnable {

        @Override
        public void run() {
            try {
                while (true) {
                    if (finished) {
                        break;
                    }

                    long currentTime = System.currentTimeMillis();
                    while (!pendingTuples.isEmpty()) {
                        TupleStatus ts = pendingTuples.get(0);
                        if (completedTuples.contains(ts.tuple.getID())) {
                            pendingTuples.remove(0);
                        } else if (currentTime - ts.timestamp > Catalog.TUPLE_TIMEOUT) {
                            pendingTuples.remove(0);
                            sendTuple(ts.tuple);
                        } else {
                            break;
                        }
                    }

                    Thread.sleep(Catalog.TIMEOUT_CHECK_GAP);
                }
            } catch (InterruptedException | IOException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }

    }

    private boolean finished;
    private final Task task;
    private final ISpout spout;
    private final Logger logger;
    private final DatagramSocket sendSocket;
    private final DatagramSocket recSocket;
    private final OutputCollector output;
    private final INimbus nimbus;
    private final Set<Integer> completedTuples;
    private final List<TupleStatus> pendingTuples;

    public SpoutWorker(Task task, Address ackerAddress, INimbus nimbus, Logger logger)
            throws SocketException {
        this.finished = false;
        this.task = task;
        this.spout = (ISpout) task.comp;
        this.recSocket = new DatagramSocket(task.getTaskAddress().port);
        this.recSocket.setReceiveBufferSize(Catalog.UDP_RECEIVE_BUFFER_SIZE);
        this.sendSocket = new DatagramSocket();
        this.output = new OutputCollector(ackerAddress, sendSocket);
        this.nimbus = nimbus;
        this.logger = logger;
        this.completedTuples = Collections.synchronizedSet(new HashSet<>());
        this.pendingTuples = Collections.synchronizedList(new LinkedList<>());
    }

    @Override
    public void setTask(Task task) {
        // can not simply reassign task, because task in Spout maintains
        // information about the position in file
        // this.task = task;

        // Because we are only changing references, it is OK to not use locks.
        for (int i = 0; i < task.comp.getChildren().size(); i++) {
            spout.getChildren().set(i, task.comp.getChildren().get(i));
        }
    }

    @Override
    public void run() {
        try {
            new Thread(new AckReceiver(recSocket)).start();
            new Thread(new TimeoutChecker()).start();

            spout.open();

            ITuple tuple;
            while ((tuple = spout.nextTuple()) != null) {
                sendTuple(tuple);
                ////////
                logger.info("Sending tuple " + tuple.getID() + " " + tuple.getContent()[0]);
            }

            spout.close();

            while (!pendingTuples.isEmpty()) {
                Thread.sleep(Catalog.FINISH_STATUS_CHECK_GAP);
            }

            nimbus.finishJob();
            this.finished = true;
            recSocket.close();
            sendSocket.close();

            logger.info(task.getTaskId() + ": terminated.");
        } catch (IOException | InterruptedException e) {
            logger.log(Level.SEVERE, e.getMessage(), e);
        }
    }

    private void sendTuple(ITuple tuple) throws IOException, InterruptedException {
        spout.execute(tuple, output);
        pendingTuples.add(new TupleStatus(tuple, System.currentTimeMillis()));
    }

    @Override
    public void terminate() {
    }
}
