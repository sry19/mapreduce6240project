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
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class kmeansCluster {

    static Configuration config = HBaseConfiguration.create();
    static String tableNm = "preprocess";
    int k = 3;
    static List<String> origins = new ArrayList<>();
    Connection connection;
    Table table;

    public static class CentroidMapper extends Mapper<Object, Text, Text, Text> {


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lst = value.toString().split("\t");
            String myid = lst[0];
            int cluster = Integer.parseInt(lst[1]);
            context.write(new Text(String.valueOf(cluster)), new Text(myid));
        }
    }

    public static class ClusterPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text text, Text text2, int i) {
            int minIndex = Integer.parseInt(text.toString());
            return minIndex % i;
        }
    }

    public static class CentroidReducer extends Reducer<Text, Text, Text, Text> {

        Connection connection;
        Table table;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            // String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
            // config.addResource(new File(hbaseSite).toURI().toURL());
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableNm));
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String minId = "0";
            Double distance = Double.MAX_VALUE;

            for (Text v : values) {
                String myid = v.toString();
                System.out.println(myid);
                byte[] myrowkey = new byte[2 * Bytes.SIZEOF_LONG];
                Bytes.putBytes(myrowkey,0,Bytes.toBytes(Long.parseLong(myid)),0,Bytes.SIZEOF_LONG);
                Get g = new Get(myrowkey);
                Result r = table.get(g);
                Helper myHelper = Helper.createHelperFromResult(r);

                Double dist = 0.0;
                for (Text v2: values) {
                    String otherid = v2.toString();
                    byte[] otherrowkey = new byte[2 * Bytes.SIZEOF_LONG];
                    Bytes.putBytes(otherrowkey,0,Bytes.toBytes(Long.parseLong(otherid)),0,Bytes.SIZEOF_LONG);
                    Get get = new Get(otherrowkey);
                    Result res = table.get(get);
                    Helper otherHelper = Helper.createHelperFromResult(res);

                    int languageDiff = 0;
                    if (!otherHelper.language.equals(myHelper.language)) {
                        languageDiff = 1;
                    }

                    dist += Math.sqrt(Math.pow(myHelper.time - otherHelper.time, 2) +
                            Math.pow(myHelper.numOfHashtags - otherHelper.numOfHashtags, 2) +
                            Math.pow(languageDiff, 2) +
                            Math.pow(myHelper.hasVideo - otherHelper.hasVideo, 2));
                }
                if (dist < distance) {
                    minId = myid;
                    distance = dist;
                }
            }
            context.write(key, new Text(minId));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            table.close();
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

        origins.add("1387557224004952070");
        origins.add("1387557219198246913");
        origins.add("1387557218057428993");

        Job job = Job.getInstance(config, "cluster2");
        job.setJarByClass(kmeansCluster.class);
        job.setMapperClass(CentroidMapper.class);
        job.setPartitionerClass(ClusterPartitioner.class);
        job.setReducerClass(CentroidReducer.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        //job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableNm);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        //job.setOutputFormatClass(TableOutputFormat.class);

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
