
import sdfs.Client;
import sdfs.OutsideClient;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

import system.Catalog;

/**
 * This spout reads data from a CSV file.
 */
public class LineSpout extends BaseRichSpout {
  private final String fileName;
  private final char separator;
  private boolean includesHeaderRow;
  private SpoutOutputCollector _collector;
  private BufferedReader reader;
  private AtomicLong linesRead;
  private int i = 0;

  public LineSpout(String filename, char separator, boolean includesHeaderRow) {
    this.fileName = filename;
    this.separator = separator;
    this.includesHeaderRow = includesHeaderRow;
    linesRead=new AtomicLong(0);
  }
  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    Logger logger = null;
    try {
      reader = new BufferedReader(new FileReader(fileName), separator); //edit dir for storm
      // read and ignore the header if one exists
      if (includesHeaderRow) reader.readLine();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void nextTuple() {
    try {
        String line = reader.readLine();
        if (line == null) {
            try {
                if (i == 3) {
                  return ;
                }
                i += 1;
                reader = new BufferedReader(new FileReader(fileName), separator); //edit dir for storm
                // read and ignore the header if one exists
                if (includesHeaderRow) reader.readLine();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        long id=linesRead.incrementAndGet();
        _collector.emit(new Values(line),id);
        
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
    System.err.println("Failed tuple with id "+id);
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    try {
        BufferedReader reader = new BufferedReader(new FileReader(fileName), separator);
      // read csv header to get field info
      String fields = reader.readLine();
      if (includesHeaderRow) {
        System.out.println("DECLARING OUTPUT FIELDS");
        System.out.println(fields);
       // declarer.declare(new Fields(Arrays.asList(fields)));
        declarer.declare(new Fields("lines"));
      } else {
        String f="line";
        declarer.declare(new Fields(f));
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

}