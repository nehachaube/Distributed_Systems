package crane.bolt;

import java.util.ArrayList;
import java.util.List;

import crane.partition.IPartitionStrategy;
import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;

public class SplitSentenceBolt extends BasicBolt {

    private static final long serialVersionUID = 1L;

    public SplitSentenceBolt(String componentID, int parallelism, IPartitionStrategy ps,
            int sendGap) {
        super(componentID, parallelism, ps, sendGap);
    }

    @Override
    public List<ITuple> map(ITuple tuple) {
        String line = (String) tuple.getContent()[0];
        String[] words = line.split("\\s+");

        List<ITuple> tuples = new ArrayList<>();
        int tupleId = tuple.getID();
        for (String word : words) {
            tuples.add(new OneStringTuple(tupleId, word));
        }

        return tuples;
    }
}
