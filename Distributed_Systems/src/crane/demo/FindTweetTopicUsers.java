package crane.demo;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import crane.INimbus;
import crane.bolt.IBolt;
import crane.bolt.TweerTopicFilterBolt;
import crane.bolt.SinkBolt;
import crane.bolt.TweetIDUserJoinBolt;
import crane.partition.RandomPartitionStrategy;
import crane.spout.FileLineSpout;
import crane.spout.ISpout;
import crane.topology.Topology;
import system.Catalog;

public class FindTweetTopicUsers {
    public static void main(String[] args)
            throws NotBoundException, IOException, InterruptedException {
        ISpout spout = new FileLineSpout("spout", "TT-annotations.csv", 1000);
        IBolt bolt1 = new TweerTopicFilterBolt("bolt-1", 1, new RandomPartitionStrategy(), 0, "ongoing-event");
        IBolt bolt2 = new TweetIDUserJoinBolt("bolt-2", 1, new RandomPartitionStrategy(), 1000);
        IBolt sink = new SinkBolt("sink", "FindTweetTopicUsers_result.txt");
        spout.addChild(bolt1);
        bolt1.addChild(bolt2);
        bolt2.addChild(sink);
        
        Topology top = new Topology("FindTweetTopicUsers", spout);

        Registry registry = LocateRegistry.getRegistry(Catalog.NIMBUS_ADDRESS, Catalog.NIMBUS_PORT);
        INimbus nimbus = (INimbus) registry.lookup("nimbus");

        nimbus.submitTopology(top);
    }
}
