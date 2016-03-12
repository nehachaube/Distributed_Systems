package crane.bolt;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;

import crane.partition.IPartitionStrategy;
import crane.tuple.ITuple;
import crane.tuple.OneStringTuple;
import system.Catalog;

public class TweetIDUserJoinBolt extends BasicBolt {

    private static final long serialVersionUID = 1L;

    public TweetIDUserJoinBolt(String componentID, int parallelism, IPartitionStrategy ps,
            int sendGap) {
        super(componentID, parallelism, ps, sendGap);
    }

    @Override
    public List<ITuple> map(ITuple tuple) throws IOException {
        OneStringTuple t = (OneStringTuple) tuple;
        String s = (String) t.getContent()[0];
        String tweetid = s.split("\t")[0];

        List<String> lines = FileUtils.readLines(new File(Catalog.CRANE_DIR + "tweets/" + tweetid),
                Catalog.ENCODING);
        List<ITuple> joinResult = new ArrayList<>();
        for (String line : lines) {
            String[] fields = line.split("\t");
            joinResult.add(new OneStringTuple(tuple.getID(), tweetid + "\t" + fields[1]));
        }
        return joinResult;
    }
}
