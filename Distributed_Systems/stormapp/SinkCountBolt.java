
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.time.LocalDateTime;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

public class SinkCountBolt extends BaseRichBolt {

    private OutputCollector collector;
    private PrintWriter out;
    @Override
    public void execute(Tuple input) {
        String topic = (String) input.getValueByField("topic");
        Integer count = (Integer) input.getValueByField("count");
        try {
            
            out.println(topic+" "+count);
            out.flush();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            System.out.println("File not created");
            e.printStackTrace();
        }
        
        
    }

    @Override
    public void prepare(Map map, TopologyContext topologContext, OutputCollector collector) {
        try {
            out = new PrintWriter(new File("/home/nchaub2/apache-storm-0.9.5/examples/storm-starter/file3.txt"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        this.collector = collector;
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweetOutput"));
        
    }

}
