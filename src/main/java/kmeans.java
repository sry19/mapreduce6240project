import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class kmeans {

    static String tableNm = "preprocess";

    public static class prepMapper extends Mapper<Object, Text, ImmutableBytesWritable, Put> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String input = value.toString();
            if (input.equals("")) {
                return;
            }
            // split lines by comma
            String reg = ",(?!\\s)";
            String[] res = input.split(reg);

            // num of hashtags
            int numOfHashtags = 0;
            if (res.length <= 23) {
                return;
            }
            if (res[18].matches("[0-9]+")) {
                numOfHashtags = Integer.parseInt(res[18]);
            } else if (!res[18].equals("[]")) {

                String origin = res[18];

//                if (origin.startsWith("\"")) {
//                    origin = origin.substring(1,origin.length()-1);
//                }
                String[] r = origin.split(",");
                numOfHashtags = r.length;
            }

            // published time
            int time = 0;
            String[] t = res[4].split(":");
            try {
                if (t.length == 3) {
                    int h = Integer.parseInt(t[0]);
                    if (h >= 5 && h <= 12) {
                        time = 1;
                    } else if (h > 12 && h <= 18) {
                        time = 2;
                    } else if (h > 20 || h <= 1) {
                        time = 3;
                    }
                }
            } catch (NumberFormatException e) {
                System.out.println(t[0]);
            }

            String hBaseKey = res[0];
            byte[] rowKey = new byte[2 * Bytes.SIZEOF_LONG];
            try {
                Bytes.putBytes(rowKey, 0, Bytes.toBytes(Long.parseLong(hBaseKey)), 0, Bytes.SIZEOF_LONG);
            } catch (NumberFormatException e) {
                return;
            }
            //long timeStamp=System.nanoTime();
            //Bytes.putLong(rowKey,Bytes.SIZEOF_LONG,timeStamp);
            Put put = new Put(rowKey);
            put.addColumn(Bytes.toBytes("id"), Bytes.toBytes("id"), Bytes.toBytes(res[0]));
            put.addColumn(Bytes.toBytes("cluster"), Bytes.toBytes("numOfHashtags"), Bytes.toBytes(String.valueOf(numOfHashtags)));
            put.addColumn(Bytes.toBytes("cluster"), Bytes.toBytes("language"), Bytes.toBytes(res[11]));
            int hasVideo = 0;
            if (res[23].matches("[0-9]+")) {
                hasVideo = Integer.parseInt(res[23]);
            } else if (res[23].equals("True")) {
                hasVideo = 1;
            }
            put.addColumn(Bytes.toBytes("cluster"), Bytes.toBytes("hasVideos"), Bytes.toBytes(String.valueOf(hasVideo)));
            int replies;
            try {
                replies = Integer.parseInt(res[15]);
            } catch (Exception e) {
                replies = 0;
            }
            put.addColumn(Bytes.toBytes("cluster"), Bytes.toBytes("replyCount"), Bytes.toBytes(String.valueOf(replies)));
            int retweets;
            try {
                retweets = Integer.parseInt(res[16]);
            } catch (Exception e) {
                retweets = 0;
            }
            put.addColumn(Bytes.toBytes("cluster"), Bytes.toBytes("retweetCount"), Bytes.toBytes(String.valueOf(retweets)));
            int likes;
            try {
                likes = Integer.parseInt(res[17]);
            } catch (Exception e) {
                likes = 0;
            }
            put.addColumn(Bytes.toBytes("cluster"), Bytes.toBytes("likeCount"), Bytes.toBytes(String.valueOf(likes)));
            put.addColumn(Bytes.toBytes("cluster"), Bytes.toBytes("time"), Bytes.toBytes(String.valueOf(time)));

            context.write(new ImmutableBytesWritable(rowKey), put);
        }

    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {

        Configuration config = HBaseConfiguration.create();
        String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
        //config.addResource(new File(hbaseSite).toURI().toURL());
        Connection conn = ConnectionFactory.createConnection(config);
        Admin hAdmin = conn.getAdmin();

        if (args.length != 2) {
            System.out.println("Usage:  <in> [<in>...] <out>");
            System.exit(2);
        }

        if (!hAdmin.tableExists(TableName.valueOf(tableNm))) {
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(TableName.valueOf(tableNm));
            tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("id")).build())
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("cluster")).build());

            TableDescriptor tableDescriptor = tableDescriptorBuilder.build();
            hAdmin.createTable(tableDescriptor);
        } else {
            System.out.println("table exists");
        }

        Job job = Job.getInstance(config, "populate");

        job.setJarByClass(kmeans.class);
        job.setMapperClass(prepMapper.class);

        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setOutputFormatClass(TableOutputFormat.class);
        job.getConfiguration().set(TableOutputFormat.OUTPUT_TABLE, tableNm);

        //job.setMapOutputKeyClass(Text.class);
        //job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(ImmutableBytesWritable.class);
        job.setOutputValueClass(Put.class);
        //job.setOutputFormatClass(NullWritable.class);
        job.setInputFormatClass(TextInputFormat.class);


        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
