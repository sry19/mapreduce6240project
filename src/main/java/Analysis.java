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
import java.util.HashMap;
import java.util.List;

public class Analysis {

    static Configuration config = HBaseConfiguration.create();
    static String tableNm = "preprocess";

    public static class AnalyzeMapper extends Mapper<Object, Text, Text, Text> {
        Connection connection;
        Table table;
        String originsStr;
        String[] originsArray;
        List<String> origins;
        int like;
        int countlike;
        int reply;
        int countreply;
        int retweet;
        int countretweet;
        int hashtag;
        int counthashtag;
        int video;
        int countvideo;

        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            config = context.getConfiguration();
            String hbaseSite = "/etc/hbase/conf/hbase-site.xml";
            //config.addResource(new File(hbaseSite).toURI().toURL());
            connection = ConnectionFactory.createConnection(config);
            table = connection.getTable(TableName.valueOf(tableNm));
            like = 0;
            countlike = 0;
            reply = 0;
            countreply = 0;
            retweet = 0;
            countretweet = 0;
            hashtag = 0;
            counthashtag = 0;
            video = 0;
            countvideo = 0;
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] lst = value.toString().split("\t");
            String id = lst[0];
            int cluster = Integer.parseInt(lst[1]);
            byte[] startrowkey = new byte[2 * Bytes.SIZEOF_LONG];
            Bytes.putBytes(startrowkey,0,Bytes.toBytes(Long.parseLong(id)),0,Bytes.SIZEOF_LONG);

            Get g = new Get(startrowkey);
            Result r = table.get(g);
            Helper originHelper = Helper.createHelperFromResult(r);

            int numOfHashtags = originHelper.numOfHashtags;
            String language = originHelper.language;
            int hasVideo = originHelper.hasVideo;
            int time = originHelper.time;
            int likee = originHelper.likeCount;
            int replyy = originHelper.replyCount;
            int retweett = originHelper.retweetCount;
            like += likee;
            countlike += 1;
            reply += replyy;
            countreply += 1;
            retweet += retweett;
            countretweet += 1;
            hashtag += numOfHashtags;
            counthashtag += 1;
            video += hasVideo;
            countvideo += 1;

            String res = String.valueOf(numOfHashtags) + "\t" + language + "\t" + String.valueOf(hasVideo) + "\t"
                    + String.valueOf(time);
            if (!language.equals("en") && !language.equals("tr")) {
                language = "others";
            }
            context.write(new Text(String.valueOf(hasVideo)), new Text(String.valueOf(1)));
        }

        @Override
        public void cleanup(Context context) throws IOException, InterruptedException {
            table.close();
            //context.write(new Text(String.valueOf(video)), new Text(String.valueOf(countvideo)));
            //context.write(new Text("hashtag"), new Text(String.valueOf(hashtag)+"\t"+String.valueOf(counthashtag)));

//            context.write(new Text("like"), new Text(String.valueOf(like)+"\t"+String.valueOf(countlike)));
//            context.write(new Text("reply"), new Text(String.valueOf(reply)+"\t"+String.valueOf(countreply)));
//            context.write(new Text("retweet"), new Text(String.valueOf(retweet)+"\t"+String.valueOf(countretweet)));
            super.cleanup(context);
        }
    }

    public static class AnalyseReducer extends Reducer<Text, Text, Text, IntWritable> {


        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);

        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            int total = 0;
            for (Text v: values) {
                count += 1;
//                String[] lst = v.toString().split("\t");
//                total += Integer.parseInt(lst[0]);
//                count += Integer.parseInt(lst[1]);
            }
//            int res = 0;
//            if (total != 0) {
//                res = total / count;
//            }
            context.write(key, new IntWritable(count));
//            int res = 0;
//            if (total != 0) {
//                res = total / div;
//            }
//            context.write(key, new IntWritable(res));
        }
    }

    public static void main(String[] otherArgs) throws IOException, InterruptedException, ClassNotFoundException {

        if (otherArgs.length != 2) {
            System.out.println("Usage:  <in> [<in>...] <out>");
            System.exit(2);
        }


        Job job = Job.getInstance(config, "analyze");
        job.setJarByClass(Analysis.class);
        job.setMapperClass(AnalyzeMapper.class);
        job.setReducerClass(AnalyseReducer.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.waitForCompletion(true);
    }
}
