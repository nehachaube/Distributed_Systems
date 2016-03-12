

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.ShellBolt;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CountTopicBolt extends BaseRichBolt {
    private OutputCollector collector;
        Map<String, Integer> counts = new HashMap<String, Integer>();
        @Override
        public void execute(Tuple tuple) {
          String topic = (String)tuple.getValueByField("topic");
          Integer count = counts.get(topic);
          if (count == null)
            count = 0;
          count++;
          counts.put(topic, count);
          collector.emit(tuple,new Values(topic, count));
          System.out.println(topic+": "+count);
          collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
          declarer.declare(new Fields("topic", "count"));
        }

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
            this.collector = collector;
            
        }


}
