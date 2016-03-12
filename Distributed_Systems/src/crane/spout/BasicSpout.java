package crane.spout;

import java.io.IOException;

import crane.task.OutputCollector;
import crane.topology.BasicComponent;
import crane.tuple.ITuple;

public abstract class BasicSpout extends BasicComponent implements ISpout {

    public BasicSpout(String componentID, int sendGap) {
        super(componentID, 1, null, sendGap);
    }

    private static final long serialVersionUID = 1L;

    private int tupleID = 0;

    @Override
    public synchronized void execute(ITuple tuple, OutputCollector output) throws IOException, InterruptedException {
        long checksum = 0;
        tuple.setID(tupleID++);
        checksum = output.emit(tuple, this, checksum);
        output.ack(tuple.getID(), checksum);
        if (sendGap > 0) {
            Thread.sleep(sendGap);
        }
    }
}
