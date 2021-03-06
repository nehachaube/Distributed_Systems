

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class FilterTopics2Bolt extends BaseRichBolt{

    private OutputCollector collector;
    private PrintWriter out;

    @Override
    public void execute(Tuple input) {
        String line=(String)input.getValueByField("lines");
        String[] words = line.split(";");
        String tweetid = words[0];
        String topic = words[3];
        if ("ongoing-event".equalsIgnoreCase(topic) || "news".equalsIgnoreCase(topic) || "meme".equalsIgnoreCase(topic)) {
            collector.emit(input,new Values(tweetid, topic));
            System.out.println(topic+":tweet "+tweetid+" for topic "+topic);     
                out.println(tweetid+" for "+topic);
                out.flush();
            
        }
        collector.ack(input);
        
       // }
    }
 

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        try {
            out = new PrintWriter(new File("/home/nchaub2/apache-storm-0.9.5/examples/storm-starter/log1.txt"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweetid", "topic"));
        
    }
    
}
