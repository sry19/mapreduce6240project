import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Cluster {

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
                Math.pow((helper1.numOfHashtags - helper2.numOfHashtags)*0.8, 2) +
                Math.pow(languageDiff, 2) +
                Math.pow(helper1.hasVideo - helper2.hasVideo, 2));
        return distance;
    }

    public static class ClusterMapper extends Mapper<Object, Text, Text, Text> {
        Connection connection;
        Table table;
        String originsStr;
        String[] originsArray;
        List<String> origins;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            config = context.getConfiguration();
            String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
            config.addResource(new File(hbaseSite).toURI().toURL());
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableNm));
            originsStr = config.get("originsStr");
            System.out.println("originstr:"+originsStr);
            originsArray = originsStr.split(",");
            origins = Arrays.asList(originsArray);
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lst = value.toString().split("\t");
            String idStart = lst[0] + "000000000000000";
            byte[] startrowkey = new byte[2 * Bytes.SIZEOF_LONG];
            Bytes.putBytes(startrowkey,0,Bytes.toBytes(Long.parseLong(idStart)),0,Bytes.SIZEOF_LONG);
            byte[] startScanKey = startrowkey;
            String idStop = lst[0] + "199999999999999";
            byte[] stoprowkey = new byte[2 * Bytes.SIZEOF_LONG];
            Bytes.putBytes(stoprowkey,0,Bytes.toBytes(Long.parseLong(idStop)),0,Bytes.SIZEOF_LONG);
            byte[] stopScanKey = stoprowkey;

            Scan scan = new Scan(startScanKey, stopScanKey);
            scan.setCaching(1000);
            scan.setCacheBlocks(false);

            ResultScanner resultScanner = table.getScanner(scan);
            int count = 0;
            for (Result result : resultScanner) {
                count += 1;
                byte[] idByte = result.getValue(Bytes.toBytes("id"), Bytes.toBytes("id"));
                String id = new String(idByte);

                Helper myHelper = Helper.createHelperFromResult(result);

                ArrayList<Double> distances = new ArrayList<>();

                if (origins.size() != 3) {
                    throw new IndexOutOfBoundsException("xx:ss"+origins.size());
                }
                for (String origin: origins) {
                    byte[] rowkey = new byte[2 * Bytes.SIZEOF_LONG];
                    Bytes.putBytes(rowkey,0,Bytes.toBytes(Long.parseLong(origin)),0,Bytes.SIZEOF_LONG);

                    Get g = new Get(rowkey);
                    Result r = table.get(g);

                    Helper originHelper = Helper.createHelperFromResult(r);

                    double distance = calculateDistance(originHelper, myHelper);

                    distances.add(distance);
                }

                Double minDistance = distances.get(0);
                int minIndex = 0;
                for (int i = 1; i < distances.size(); i++) {
                    if (distances.get(i) < minDistance) {
                        minIndex = i;
                    }
                }

                context.write(new Text(String.valueOf(minIndex)), new Text(id));
            }
            System.out.println(count);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            table.close();
            super.cleanup(context);
        }
    }

    public static class ClusterPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text text, Text text2, int i) {
            int minIndex = Integer.parseInt(text.toString());
            return minIndex % i;
        }
    }

    public static class ClusterReducer extends Reducer<Text, Text, Text, IntWritable> {

        Connection connection;
        Table table;
        //FileWriter writer;
        String originsStr;
        String[] originsArray;
        List<String> origins;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            config = context.getConfiguration();
            String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
            config.addResource(new File(hbaseSite).toURI().toURL());
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableNm));
            originsStr = config.get("originsStr");
            System.out.println("originstr:"+originsStr);
            originsArray = originsStr.split(",");
            origins = Arrays.asList(originsArray);
            //writer = new FileWriter(file);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double distance = 0.0;
            String cid = origins.get(Integer.parseInt(key.toString()));
            byte[] cidrowkey = new byte[2 * Bytes.SIZEOF_LONG];
            Bytes.putBytes(cidrowkey,0,Bytes.toBytes(Long.parseLong(cid)),0,Bytes.SIZEOF_LONG);
            Get getcid = new Get(cidrowkey);
            Result rescid = table.get(getcid);
            Helper cidHelper = Helper.createHelperFromResult(rescid);

            for (Text v : values) {
                String curid = v.toString();
                byte[] currowkey = new byte[2 * Bytes.SIZEOF_LONG];
                Bytes.putBytes(currowkey,0,Bytes.toBytes(Long.parseLong(curid)),0,Bytes.SIZEOF_LONG);
                Get getcur = new Get(currowkey);
                Result rescur = table.get(getcur);
                Helper curHelper = Helper.createHelperFromResult(rescur);

                distance += calculateDistance(cidHelper, curHelper);

                context.write(v, new IntWritable(Integer.parseInt(key.toString())));
            }
            //writer.write("total distance"+ k+":"+key.toString() +","+ distance+"\n");
            //writer.flush();
            //System.out.println("total distance:"+ key.toString() + distance);
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            table.close();
            super.cleanup(context);
            //writer.close();
        }

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
            config.addResource(new File(hbaseSite).toURI().toURL());
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
            Double distance = Double.MAX_VALUE;

            for (Text v : values) {
                String myid = v.toString();
                System.out.println(myid);
                byte[] myrowkey = new byte[2 * Bytes.SIZEOF_LONG];
                Bytes.putBytes(myrowkey,0, Bytes.toBytes(Long.parseLong(myid)),0,Bytes.SIZEOF_LONG);
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

                    dist += calculateDistance(myHelper, otherHelper);

                }
                if (dist < distance) {
                    minId = myid;
                    distance = dist;
                }
            }
            porigins.set(cluster, minId);
            //config.unset("origins"+cluster);
            //config.set("origins"+cluster, minId);
            context.write(key, new Text(minId));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            System.out.println("size:"+porigins.toString());
            for (int i=0; i<porigins.size(); i++) {
                config.set("origins"+i, porigins.get(i));
            }
            table.close();
            super.cleanup(context);
        }
    }

    public static void main(String[] otherArgs) throws IOException, InterruptedException, ClassNotFoundException {
//        Connection connection = ConnectionFactory.createConnection(config);
//        Admin hAdmin = connection.getAdmin();

        if (otherArgs.length != 6) {
            System.out.println("Usage:  <in> [<in>...] <out>");
            System.exit(2);
        }

        k = Integer.parseInt(otherArgs[4]);
        List<String> origins = new ArrayList<>();
        String s1 = "1355065891633975299";
        String s2 = "1374090777605906436";
        String s3 = "1345000023092695040";
        origins.add(s1);
        origins.add(s2);
        origins.add(s3);
        preOrigins.add(s1);
        preOrigins.add(s2);
        preOrigins.add(s3);

        System.out.println(origins);

        // delete existing contents
        //new PrintWriter(otherArgs[2]).close();
        String com = String.join(",",origins);
        config.set("originsStr", com);
        config.set("origins0", s1);
        config.set("origins1", s2);
        config.set("origins2", s3);
        for (int idx = 4; idx < 5; idx ++) {
            System.out.println("originsStr:"+config.get("originsStr"));
            System.out.println("origins0:"+config.get("origins0"));
            System.out.println("origins1:"+config.get("origins1"));
            System.out.println("origins2:"+config.get("origins2"));
            Job job = Job.getInstance(config, "cluster"+String.valueOf(idx));
            job.setJarByClass(Cluster.class);
            job.setMapperClass(ClusterMapper.class);
            job.setPartitionerClass(ClusterPartitioner.class);
            job.setReducerClass(ClusterReducer.class);
            FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
            FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]+String.valueOf(idx)));
            //job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableNm);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.waitForCompletion(true);

            // job2
            Job job2 = Job.getInstance(config, "recal"+ String.valueOf(idx));
            job2.setJarByClass(Cluster.class);
            job2.setMapperClass(CentroidMapper.class);
            job2.setPartitionerClass(ClusterPartitioner.class);
            job2.setReducerClass(CentroidReducer.class);

            FileInputFormat.addInputPath(job2, new Path(otherArgs[1]+String.valueOf(idx)));
            FileOutputFormat.setOutputPath(job2, new Path(otherArgs[3] + String.valueOf(idx)));

            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.waitForCompletion(true);

            for (int i=0; i < k; i++) {
                try {
                    String o = config.get("origins" + i);
                    System.out.println("cluster"+i+":"+o);
                    origins.set(i, o);
                } catch (Exception ignored) {
                }
            }
            String n = String.join(",", origins);
            config.set("originsStr", n);
            String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
            config.addResource(new File(hbaseSite).toURI().toURL());
            Connection connection = ConnectionFactory.createConnection(config);
            Table table = connection.getTable(TableName.valueOf(tableNm));
            int flag = 1;
            for (int c=0; c<k; c++) {
                String preid = preOrigins.get(c);
                System.out.println("origins:"+origins.toString());
                String curid = origins.get(c);
                System.out.println(curid);
                byte[] prerowkey = new byte[2 * Bytes.SIZEOF_LONG];
                byte[] currowkey = new byte[2 * Bytes.SIZEOF_LONG];
                Bytes.putBytes(prerowkey,0,Bytes.toBytes(Long.parseLong(preid)),0,Bytes.SIZEOF_LONG);
                Bytes.putBytes(currowkey,0,Bytes.toBytes(Long.parseLong(curid)),0,Bytes.SIZEOF_LONG);
                Get getpre = new Get(prerowkey);
                Get getcur = new Get(currowkey);
                Result respre = table.get(getpre);
                Result rescur = table.get(getcur);
                Helper preHelper = Helper.createHelperFromResult(respre);
                Helper curHelper = Helper.createHelperFromResult(rescur);

                double dist = calculateDistance(preHelper, curHelper);

                System.out.println("dist:"+dist);
                if (dist > limit) {
                    flag = 0;
                }
            }
            if (flag == 1) {
                System.out.println("stop early");
                break;
            }
            for (int c = 0; c < k; c ++) {
                preOrigins.set(c, origins.get(c));
            }
        }
         //System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
