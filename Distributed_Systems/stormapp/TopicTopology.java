

import java.time.LocalDateTime;

import system.Catalog;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.topology.TopologyBuilder;

public class TopicTopology {

    static final String TOPOLOGY_NAME = "storm-twitter-topic";
    public static void main(String[] args) throws Exception, InvalidTopologyException {
        Config config = new Config();
        config.setMessageTimeoutSecs(120);
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("LineSpout", new LineSpout("/home/nchaub2/apache-storm-0.9.5/examples/storm-starter/TT-annotations.csv",';',true));
        builder.setBolt("FilterBolt", new FilterBolt()).shuffleGrouping("LineSpout");
        builder.setBolt("JoinBolt", new JoinBolt("\t",false)).shuffleGrouping("FilterBolt");
        builder.setBolt("SinkBolt",new SinkBolt()).shuffleGrouping("JoinBolt");
       // builder.setBolt("SinkBolt",new SinkBolt()).shuffleGrouping("LineSpout");

        
        config.setDebug(true);


        if (args != null && args.length > 0) {
          config.setNumWorkers(3);
          System.out.println("Enter distributed cluster"+" "+LocalDateTime.now());
          StormSubmitter.submitTopology(args[0], config, builder.createTopology());
        }
        else {
            System.out.println("Enter local cluster");
          config.setMaxTaskParallelism(3);
        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology());

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                cluster.killTopology(TOPOLOGY_NAME);
                cluster.shutdown();
            }
        });
    }
    }
}
