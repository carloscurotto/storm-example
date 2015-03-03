package ar.com.carloscurotto.storm.updates.kafka;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import ar.com.carloscurotto.storm.updates.fixed.repository.GlossUpdateCountsRepository;
import ar.com.carloscurotto.storm.updates.fixed.repository.HBaseUpdateCountsRepository;
import ar.com.carloscurotto.storm.updates.fixed.repository.HazelcastInstanceProvider;
import ar.com.carloscurotto.storm.wordcount.kafka.KafkaSentenceScheme;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class UpdateCountExample {

    private static IRichSpout createKafkaSpout() {
        BrokerHosts hosts = new ZkHosts("localhost:2181");
        String topic = "test";
        String zkRoot = "/kafkastorm";
        String consumerGroupId = "StormSpout";
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, consumerGroupId);
        spoutConfig.scheme = new SchemeAsMultiScheme(new KafkaSentenceScheme());
        return new KafkaSpout(spoutConfig);
    }

    public static void main(String[] args) throws Exception {

        HazelcastInstanceProvider.start();

        GlossUpdateCountsRepository glossRepository = new GlossUpdateCountsRepository();

        HBaseUpdateCountsRepository hbaseRepository = new HBaseUpdateCountsRepository();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", createKafkaSpout(), 1);
        builder.setBolt("router", new RouterUpdatesBolt(), 1).fieldsGrouping("spout", new Fields("update"));
        builder.setBolt("gloss", new GlossUpdatesBolt(glossRepository), 1).fieldsGrouping("router", "gloss-stream", new Fields("update"));
        builder.setBolt("hbase", new HBaseUpdatesBolt(hbaseRepository), 1).fieldsGrouping("router", "hbase-stream", new Fields("update"))
                .fieldsGrouping("gloss", "hbase-stream", new Fields("update"));

        Config configuration = new Config();
        configuration.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("update-count", configuration, builder.createTopology());

        System.out.println("Press any key to stop processing...");
        System.in.read();

        cluster.killTopology("update-count");

        cluster.shutdown();

        System.out.println(glossRepository);
        System.out.println(hbaseRepository);

        HazelcastInstanceProvider.stop();
    }
}
