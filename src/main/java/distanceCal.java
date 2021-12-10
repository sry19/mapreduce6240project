import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class distanceCal {

    static int k;
    static double limit = 1.5;
    //static List<String> origins = new ArrayList<>();
    static List<String> preOrigins = new ArrayList<>();
    static Configuration config = HBaseConfiguration.create();
    static String tableNm = "preprocess";

    protected static double calculateDistance(Helper helper1, Helper helper2) {
        int languageDiff = 0;
        if (!helper1.language.equals(helper2.language)) {
            languageDiff = 1;
        }
        double distance = 0.0;
        distance = Math.sqrt(Math.pow(helper1.time - helper2.time, 2) +
                Math.pow((helper1.numOfHashtags - helper2.numOfHashtags) * 0.8, 2) +
                Math.pow(languageDiff, 2) +
                Math.pow(helper1.hasVideo - helper2.hasVideo, 2));
        return distance;
    }

    public static class CentroidMapper extends Mapper<Object, Text, Text, Text> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lst = value.toString().split("\t");
            if (lst.length < 2) {
                return;
            }
            String myid = lst[0];
            int cluster = Integer.parseInt(lst[1]);
            context.write(new Text(String.valueOf(cluster)), new Text(myid));
        }
    }

    public static class CentroidReducer extends Reducer<Text, Text, Text, Text> {

        Connection connection;
        Table table;
        String originsStr;
        String[] originsArray;
        List<String> porigins;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            config = context.getConfiguration();
            String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
            //config.addResource(new File(hbaseSite).toURI().toURL());
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableNm));
            originsStr = config.get("originsStr");
            originsArray = originsStr.split(",");
            porigins = new ArrayList<>();
            porigins = Arrays.asList(originsArray);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int cluster = Integer.parseInt(key.toString());
            String minId = porigins.get(cluster);
            System.out.println("min" + minId);

            String otherid = minId;
            byte[] otherrowkey = new byte[2 * Bytes.SIZEOF_LONG];
            Bytes.putBytes(otherrowkey, 0, Bytes.toBytes(Long.parseLong(otherid)), 0, Bytes.SIZEOF_LONG);
            Get get = new Get(otherrowkey);
            Result res = table.get(get);
            Helper otherHelper = Helper.createHelperFromResult(res);

            double dist = 0.0;
            for (Text v : values) {
                String myid = v.toString();
                System.out.println(myid);
                byte[] myrowkey = new byte[2 * Bytes.SIZEOF_LONG];
                Bytes.putBytes(myrowkey, 0, Bytes.toBytes(Long.parseLong(myid)), 0, Bytes.SIZEOF_LONG);
                Get g = new Get(myrowkey);
                Result r = table.get(g);
                Helper myHelper = Helper.createHelperFromResult(r);

                dist += calculateDistance(myHelper, otherHelper);

            }
            context.write(key, new Text(String.valueOf(dist)));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            table.close();
            super.cleanup(context);
        }
    }

    public static void main(String[] otherArgs) throws IOException, InterruptedException, ClassNotFoundException {

        if (otherArgs.length != 2) {
            System.out.println("Usage:  <in> [<in>...] <out>");
            System.exit(2);
        }

        List<String> origins = new ArrayList<>();
        String s1 = "1355065891633975299";
        String s2 = "1374090777605906436";
        String s3 = "1345000023092695040";


        origins.add(s1);
        origins.add(s2);
        origins.add(s3);


        k = origins.size();

        // delete existing contents
        //new PrintWriter(otherArgs[2]).close();
        String com = String.join(",", origins);
        config.set("originsStr", com);


        Job job = Job.getInstance(config, "clustercal");
        job.setJarByClass(Cluster4.class);
        job.setMapperClass(CentroidMapper.class);
        job.setReducerClass(CentroidReducer.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1] + k));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.waitForCompletion(true);


    }
}