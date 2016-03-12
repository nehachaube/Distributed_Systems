package crane.bolt;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import crane.partition.IPartitionStrategy;
import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;

public class TweerTopicFilterBolt extends BasicBolt {

    private static final long serialVersionUID = 1L;
    private Set<String> topics;

    public TweerTopicFilterBolt(String componentID, int parallelism, IPartitionStrategy ps,
            int sendGap, String... topics) {
        super(componentID, parallelism, ps, sendGap);
        this.topics = new HashSet<>();
        for (String topic : topics) {
            this.topics.add(topic);
        }
    }

    @Override
    public List<ITuple> map(ITuple tuple) {
        OneStringTuple t = (OneStringTuple) tuple;
        String line = (String) t.getContent()[0];
        String[] fields = line.split(";");
        String topic = fields[3];
        if (topics.contains(topic)) {
            return Collections
                    .singletonList(new OneStringTuple(tuple.getID(), fields[0] + "\t" + topic));
        } else {
            return Collections.emptyList();
        }
    }

}
