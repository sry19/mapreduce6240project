import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class test {
    static int k;
    static double limit = 1.5;
    static List<String> origins = new ArrayList<>();
    static List<String> preOrigins = new ArrayList<>();
    static Configuration config = HBaseConfiguration.create();
    static String tableNm = "preprocess";

    public static class ClusterMapper extends Mapper<Object, Text, Text, IntWritable> {
        Connection connection;
        Table table;
        int count;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            config = context.getConfiguration();
            //String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
            //config.addResource(new File(hbaseSite).toURI().toURL());
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableNm));
            count = 0;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lst = value.toString().split("\t");
            count += Integer.parseInt(lst[1]);

//            String idStart = lst[0] + "000000000000000";
//            byte[] startrowkey = new byte[2 * Bytes.SIZEOF_LONG];
//            Bytes.putBytes(startrowkey,0,Bytes.toBytes(Long.parseLong(idStart)),0,Bytes.SIZEOF_LONG);
//            byte[] startScanKey = startrowkey;
//            String idStop = lst[0] + "199999999999999";
//            byte[] stoprowkey = new byte[2 * Bytes.SIZEOF_LONG];
//            Bytes.putBytes(stoprowkey,0,Bytes.toBytes(Long.parseLong(idStop)),0,Bytes.SIZEOF_LONG);
//            byte[] stopScanKey = stoprowkey;
//
//            Scan scan = new Scan(startScanKey, stopScanKey);
//            scan.setCaching(1000);
//            scan.setCacheBlocks(false);
//
//            ResultScanner resultScanner = table.getScanner(scan);
//            int count = 0;
//            for (Result result : resultScanner) {
//                count += 1;
//            }
//            context.write(new Text("xx"), new IntWritable(count));
//            System.out.println("count:"+count);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            table.close();
            context.write(new Text("xx"), new IntWritable(count));
            super.cleanup(context);
        }
    }

    public static class ClusterReducer extends Reducer<Text, Text, Text, IntWritable> {

        Connection connection;
        Table table;
        String cluster;
        List<String> origins;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //config.unset("origin");
            int count = 0;
            for (IntWritable v: values) {
                count += v.get();
            }
            context.write(new Text("count"), new IntWritable(count));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {

            super.cleanup(context);
        }
    }

    public static void main(String[] otherArgs) throws IOException, InterruptedException, ClassNotFoundException {
//        Connection connection = ConnectionFactory.createConnection(config);
//        Admin hAdmin = connection.getAdmin();

        if (otherArgs.length != 2) {
            System.out.println("Usage:  <in> [<in>...] <out>");
            System.exit(2);
        }


        for (int idx = 0; idx < 1; idx++) {
            Job job = Job.getInstance(config, "cluster" + String.valueOf(idx));
            job.setJarByClass(test.class);
            job.setMapperClass(ClusterMapper.class);
            job.setReducerClass(ClusterReducer.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + String.valueOf(idx)));
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            job.waitForCompletion(true);

        }
    }
}
