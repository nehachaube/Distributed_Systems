

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import sdfs.Client;
import sdfs.OutsideClient;
import system.Catalog;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class JoinBolt extends BaseRichBolt{
    OutputCollector collector;
    private final String separator;
    private boolean includesHeaderRow;
    private BufferedReader reader;
    private AtomicLong linesRead;
    private PrintWriter out;
    public JoinBolt(String separator, boolean includesHeaderRow) {
        this.separator = separator;
        this.includesHeaderRow = includesHeaderRow;
        linesRead=new AtomicLong(0);
      }

    @Override
    public void execute(Tuple input) {
        String tweetid = (String) input.getValueByField("tweetid");
       
            out.println(tweetid);
         
        
          try {
            reader = new BufferedReader(new FileReader("/home/nchaub2/apache-storm-0.9.5/examples/storm-starter/tweets/"+tweetid));
         //edit dir for storm
          // read and ignore the header if one exists
          if (includesHeaderRow) reader.readLine();
          String line;
          while ((line = reader.readLine())!= null) {
              long id=linesRead.incrementAndGet();
              collector.emit(input,new Values(line));
          }
          collector.ack(input);
            System.out.println("Finished reading file, "+linesRead.get()+" lines read");
          }
          catch (IOException e) {
              // TODO Auto-generated catch block
              e.printStackTrace();
          }
    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
        try {
            out = new PrintWriter(new File("/home/nchaub2/apache-storm-0.9.5/examples/storm-starter/log.txt"));
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("tweetUsers"));
        
    }

}
