import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

/**
 * part C
 */
public class Top10Miners {

    public static void main(String[] args) {
        String blocks = args[0];
        String output = args[1];

        SparkConf sparkConf = new SparkConf()
                .setAppName("Top10Miners")
                .setMaster("local");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset dsBlocks = sparkSession.read().csv(blocks);
        dsBlocks.registerTempTable("blocks");
        Dataset res = dsBlocks.sqlContext().sql("select _c2 as miner, count(1) as cnt from blocks where _c0 != 'number' group by _c2 order by cnt desc limit 10");
        res.write().csv(output);
    }

}
