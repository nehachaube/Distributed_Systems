package crane.demo;

import java.io.IOException;
import java.rmi.NotBoundException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

import crane.INimbus;
import crane.bolt.CountBolt;
import crane.bolt.IBolt;
import crane.bolt.SinkBolt;
import crane.bolt.TweerTopicFilterBolt;
import crane.partition.HashPartitionStrategy;
import crane.partition.RandomPartitionStrategy;
import crane.spout.CircularFileLineSpout;
import crane.spout.ISpout;
import crane.topology.Topology;
import system.Catalog;

public class TweetTopicCount {

    public static void main(String[] args)
            throws IOException, InterruptedException, NotBoundException {
        ISpout spout = new CircularFileLineSpout("spout", "TT-annotations.csv", 0);
        IBolt bolt1 = new TweerTopicFilterBolt("bolt-1", 1, new RandomPartitionStrategy(), 0,
                "ongoing-event", "news", "meme");
        IBolt bolt2 = new CountBolt("bolt-2", 1, new HashPartitionStrategy(), 0);
        IBolt sink = new SinkBolt("sink", "TweetTopicCount_result.txt");
        spout.addChild(bolt1);
        bolt1.addChild(bolt2);
        bolt2.addChild(sink);

        Topology top = new Topology("TweetTopicCount", spout);

        Registry registry = LocateRegistry.getRegistry(Catalog.NIMBUS_ADDRESS, Catalog.NIMBUS_PORT);
        INimbus nimbus = (INimbus) registry.lookup("nimbus");

        nimbus.submitTopology(top);
    }

}
