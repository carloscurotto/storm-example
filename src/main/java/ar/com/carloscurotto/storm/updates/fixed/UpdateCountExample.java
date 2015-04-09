package ar.com.carloscurotto.storm.updates.fixed;

import ar.com.carloscurotto.storm.updates.fixed.repository.GlossUpdateCountsRepository;
import ar.com.carloscurotto.storm.updates.fixed.repository.HBaseUpdateCountsRepository;
import ar.com.carloscurotto.storm.updates.fixed.repository.HazelcastInstanceProvider;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

public class UpdateCountExample {

    public static void main(String[] args) throws Exception {

        HazelcastInstanceProvider.start();

        String[] updates =
                new String[] { "gloss-update1", "gloss-update2", "hbase-update3", "hbase-update4", "hbase-update5" };

        GlossUpdateCountsRepository glossRepository = new GlossUpdateCountsRepository();

        HBaseUpdateCountsRepository hbaseRepository = new HBaseUpdateCountsRepository();

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("spout", new FixedUpdatesSpout(updates), 1);
        builder.setBolt("gloss", new GlossUpdatesBolt(glossRepository), 1).fieldsGrouping("spout", "gloss-stream", new Fields("update"));
        builder.setBolt("hbase", new HBaseUpdatesBolt(hbaseRepository), 1).fieldsGrouping("spout", "hbase-stream", new Fields("update"))
                .fieldsGrouping("gloss", "hbase-stream", new Fields("update"));

        Config configuration = new Config();
        configuration.setDebug(true);

        LocalCluster cluster = new LocalCluster();

        cluster.submitTopology("update-count", configuration, builder.createTopology());
        Thread.sleep(10000);
        cluster.killTopology("update-count");

        cluster.shutdown();

        System.out.println(glossRepository);
        System.out.println(hbaseRepository);

        HazelcastInstanceProvider.stop();
    }
}
