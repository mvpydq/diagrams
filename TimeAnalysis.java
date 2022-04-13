import java.util.Calendar;
import java.util.Date;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

/**
 * part A
 */
public class TimeAnalysis {

    public static void main(String[] args) {
        String input = args[0];
        String output = args[1];

        SparkConf sparkConf = new SparkConf()
                .setAppName("TimeAnalysis")
                .setMaster("local");

        SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
        Dataset ds = sparkSession.read().csv(input);
        sparkSession.udf().register("get_month", new MonthUdf(), DataTypes.IntegerType);
        ds.registerTempTable("transactions");
        Dataset res = ds.sqlContext().sql("select get_month(_c6) as month, count(1) as cnt from transactions where _c0 != 'block_number' group by month");
        res.write().csv(output);
    }

    public static class MonthUdf implements UDF1<String, Integer> {

        @Override
        public Integer call(String s) throws Exception {
            Calendar calendar = Calendar.getInstance();
            long timestamp = Long.parseLong(s) * 1000;

            calendar.setTime(new Date(timestamp));
            System.out.println(timestamp + "\t" + (calendar.get(Calendar.MONTH) + 1));
            return calendar.get(Calendar.MONTH) + 1;
        }
    }
}
