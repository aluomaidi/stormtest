/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.uucun.topology;

import com.uucun.bolt.WordCounter;
import com.uucun.bolt.WordNormalizer;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.Arrays;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class WordCountTopology {

  public static void main(String[] args) throws InterruptedException, InvalidTopologyException, AuthorizationException, AlreadyAliveException {
    String zks = "10.1.69.72:2182,10.1.69.73:2182,10.1.69.74:2182";
    String topic = "test";
    String zkRoot = "/storm/topic"; // default zookeeper root configuration for storm
    String id = "word";

    BrokerHosts brokerHosts = new ZkHosts(zks);
    SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
    spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
//    spoutConf.zkServers = Arrays.asList(new String[] {"10.1.69.72", "10.1.69.73", "10.1.69.74"});
//    spoutConf.zkPort = 2182;
    TopologyBuilder builder = new TopologyBuilder();
    builder.setSpout("kafka-spout",new KafkaSpout(spoutConf), 3);
    builder.setBolt("word-normalizer", new WordNormalizer(), 2)
            .shuffleGrouping("kafka-spout");
    builder.setBolt("word-counter", new WordCounter(),2)
            .fieldsGrouping("word-normalizer", new Fields("word"));
    //Configuration
    Config conf = new Config();
    conf.setDebug(false);
    //Topology run
    conf.setMaxSpoutPending(1000);
    LocalCluster cluster = new LocalCluster();
    cluster.submitTopology("uu-toplogie", conf, builder.createTopology());
    Thread.sleep(50000);
    cluster.shutdown();
    /*Config conf = new Config();
    conf.setDebug(false);
    conf.setNumWorkers(12);
    conf.setMaxSpoutPending(1000);
    StormSubmitter.submitTopology("uu-toplogie", conf, builder.createTopology());*/
  }
}
