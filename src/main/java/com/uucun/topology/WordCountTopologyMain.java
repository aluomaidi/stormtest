package com.uucun.topology;

import com.uucun.bolt.WordCounter;
import com.uucun.bolt.WordNormalizer;
import com.uucun.spout.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;


/**
 * Created by admin on 2016/5/8.
 */
public class WordCountTopologyMain {
    public static void main(String[] args) throws InterruptedException {
        //Topology definition
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader(), 3);
        builder.setBolt("word-normalizer", new WordNormalizer(), 2)
                .shuffleGrouping("word-reader");
        builder.setBolt("word-counter", new WordCounter(),1)
                .fieldsGrouping("word-normalizer", new Fields("word"));

        //Configuration
        Config conf = new Config();
        conf.put("wordsFile", "f:/doc/words.txt");
        conf.setDebug(false);
        //Topology run
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("uu-toplogie", conf, builder.createTopology());
        Thread.sleep(50000);
        cluster.shutdown();

    }
}
