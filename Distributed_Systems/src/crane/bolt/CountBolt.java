package crane.bolt;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import crane.partition.IPartitionStrategy;
import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;

public class CountBolt extends BasicBolt {

    private Map<String, Integer> counter;

    public CountBolt(String componentID, int parallelism, IPartitionStrategy ps, int sendGap) {
        super(componentID, parallelism, ps, sendGap);
        counter = new HashMap<>();
    }

    private static final long serialVersionUID = 1L;

    @Override
    public List<ITuple> map(ITuple tuple) throws IOException {
        OneStringTuple t = (OneStringTuple) tuple;
        String s = (String) t.getContent()[0];
        String topic = s.split("\t")[1];
        counter.put(topic, counter.getOrDefault(topic, 0) + 1);
        return Collections.singletonList(
                new OneStringTuple(tuple.getID(), topic + "\t" + counter.get(topic)));
    }

}
