import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Random;

public class random {

    static Configuration config = HBaseConfiguration.create();
    static String tableNm = "preprocess";
    static int k;
    static File file;

    public static class RandomMapper extends Mapper<Object, Text, Text, Text> {
        Connection connection;
        Table table;
        Random rand;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            //String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
            //config.addResource(new File(hbaseSite).toURI().toURL());
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableNm));
            rand = new Random();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lst = value.toString().split("\t");
            String idStart = lst[0] + "000000000000000";
            byte[] startrowkey = new byte[2 * Bytes.SIZEOF_LONG];
            Bytes.putBytes(startrowkey,0,Bytes.toBytes(Long.parseLong(idStart)),0,Bytes.SIZEOF_LONG);
            byte[] startScanKey = startrowkey;
            String idStop = lst[0] + "999999999999999";
            byte[] stoprowkey = new byte[2 * Bytes.SIZEOF_LONG];
            Bytes.putBytes(stoprowkey,0,Bytes.toBytes(Long.parseLong(idStop)),0,Bytes.SIZEOF_LONG);
            byte[] stopScanKey = stoprowkey;

            Scan scan = new Scan(startScanKey, stopScanKey);
            scan.setCaching(1000);
            scan.setCacheBlocks(false);

            ResultScanner resultScanner = table.getScanner(scan);
            for (Result result : resultScanner) {
                int randomNum = rand.nextInt(5);
                if (randomNum >= 3) {
                    return;
                }
                byte[] idByte = result.getValue(Bytes.toBytes("id"), Bytes.toBytes("id"));
                String id = new String(idByte);
                context.write(new Text("1"), new Text(id));
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            table.close();
            super.cleanup(context);
        }
    }

    public static class RandomReducer extends Reducer<Text, Text, NullWritable, NullWritable> {

        ArrayList<String> randomList = new ArrayList<>();
        Random rand = new Random();
        FileWriter writer;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            writer = new FileWriter(file);
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text v : values) {
                randomList.add(v.toString());
            }
            int len = randomList.size();
            HashSet<String> set = new HashSet<>();
            int count = 0;
            for (int i=0; i < len; i++) {
                int randomNum = rand.nextInt(len);
                String cur = randomList.get(randomNum);
                if (!set.contains(cur)) {
                    writer.write(cur+"\n");
                    System.out.println("yy");
                    writer.flush();
                    //context.write(new Text(cur), new IntWritable(1));
                    set.add(cur);
                    count += 1;
                }
                if (count == k) {
                    break;
                }
            }
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            writer.close();
            super.cleanup(context);
        }
    }

    public static void main(String[] otherArgs) throws IOException, InterruptedException, ClassNotFoundException {
        if (otherArgs.length != 4) {
            System.out.println("Usage:  <in> [<in>...] <out>");
            System.exit(2);
        }
        k = Integer.parseInt(otherArgs[3]);
        file = new File(otherArgs[1]);
        if (file.createNewFile()) {
            System.out.println("File created: " + file.getName());
        } else {
            System.out.println("File already exists.");
        }
        Job job = Job.getInstance(config, "random");
        job.setJarByClass(random.class);
        job.setMapperClass(RandomMapper.class);
        job.setReducerClass(RandomReducer.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
        //job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableNm);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}