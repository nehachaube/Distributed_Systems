package crane.task;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Logger;

import crane.bolt.SinkBolt;
import crane.topology.Address;
import crane.tuple.ITuple;
import system.Catalog;

public class BoltWorker implements CraneWorker {

    private class TupleReceiver implements Runnable {

        @Override
        public void run() {
            DatagramPacket packet = new DatagramPacket(new byte[Catalog.MAX_UDP_PACKET_BYTES],
                    Catalog.MAX_UDP_PACKET_BYTES);
            try {
                while (true) {
                    receiveSocket.receive(packet);
                    ByteArrayInputStream bais = new ByteArrayInputStream(packet.getData());
                    ITuple tuple = (ITuple) new ObjectInputStream(bais).readObject();

                    upstreamTuples.addLast(tuple);
                }
            } catch (IOException | ClassNotFoundException e) {
            }
        }

    }

    private boolean finished;
    private final Task task;
    private final DatagramSocket sendSocket, receiveSocket;
    private final Logger logger;
    private final OutputCollector output;
    private final BlockingDeque<ITuple> upstreamTuples;

    public BoltWorker(Task task, Address ackerAddress, Logger logger) throws SocketException {
        this.finished = false;
        this.task = task;
        this.receiveSocket = new DatagramSocket(task.getTaskAddress().port);
        this.receiveSocket.setReceiveBufferSize(Catalog.UDP_RECEIVE_BUFFER_SIZE);
        this.sendSocket = new DatagramSocket();
        this.output = new OutputCollector(ackerAddress, sendSocket);
        this.upstreamTuples = new LinkedBlockingDeque<>();
        this.logger = logger;
    }

    @Override
    public void setTask(Task task) {
        for (int i = 0; i < task.comp.getChildren().size(); i++) {
            this.task.comp.getChildren().set(i, task.comp.getChildren().get(i));
        }
    }

    @Override
    public void run() {
        new Thread(new TupleReceiver()).start();
        
        try {
            if (task.comp instanceof SinkBolt) {
                ((SinkBolt) task.comp).open();
            }
            
            while (true) {
                if (finished) {
                    return;
                }
                ITuple tuple = upstreamTuples.pollFirst(Catalog.FINISH_STATUS_CHECK_GAP,
                        Catalog.TIME_UNIT);
                
                if (tuple != null) {
                    ////////
                    logger.info("Received tuple " + tuple.getID() + " " + tuple.getContent()[0]);
                    task.comp.execute(tuple, output);
                }
            }
        } catch (IOException | InterruptedException e) {
            logger.info(task.getTaskId() + ": terminated.");
        }
    }

    @Override
    public void terminate() {
        this.finished = true;
        this.receiveSocket.close();
        this.sendSocket.close();
        if (task.comp instanceof SinkBolt) {
            ((SinkBolt) task.comp).close();
        }
    }
}
