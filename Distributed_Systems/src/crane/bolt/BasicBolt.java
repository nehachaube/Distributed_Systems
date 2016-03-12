package crane.bolt;

import java.io.IOException;
import java.util.List;

import crane.partition.IPartitionStrategy;
import crane.task.OutputCollector;
import crane.topology.BasicComponent;
import crane.tuple.ITuple;

public abstract class BasicBolt extends BasicComponent implements IBolt {

    public BasicBolt(String componentID, int parallelism, IPartitionStrategy ps, int sendGap) {
        super(componentID, parallelism, ps, sendGap);
    }

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(ITuple tuple, OutputCollector output)
            throws IOException, InterruptedException {
        long checksum = tuple.getSalt();
        List<ITuple> tuples = map(tuple);
        ////long checksum_
        for (ITuple t : tuples) {
            checksum = output.emit(t, this, checksum);
            if (sendGap > 0) {
                Thread.sleep(sendGap);
            }
        }
        output.ack(tuple.getID(), checksum);
    }
}
