import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class Helper {

    int numOfHashtags;
    String language;
    int hasVideo;
    int replyCount;
    int retweetCount;
    int likeCount;
    int time;

    public Helper(int numOfHashtags, String language, int hasVideo, int replyCount, int retweetCount, int likeCount, int time) {
        this.numOfHashtags = numOfHashtags;
        this.language = language;
        this.hasVideo = hasVideo;
        this.replyCount = replyCount;
        this.retweetCount = retweetCount;
        this.likeCount = likeCount;
        this.time = time;
    }

    public static Helper createHelperFromResult(Result result) {
        byte[] numOfHashtagsByte = result.getValue(Bytes.toBytes("cluster"), Bytes.toBytes("numOfHashtags"));
        int numOfHashtags = Integer.parseInt(new String(numOfHashtagsByte));
        byte[] languageByte = result.getValue(Bytes.toBytes("cluster"), Bytes.toBytes("language"));
        String language = new String(languageByte);
        byte[] hasVideosByte = result.getValue(Bytes.toBytes("cluster"), Bytes.toBytes("hasVideos"));
        int hasVideo = Integer.parseInt(new String(hasVideosByte));
        byte[] replyCountByte = result.getValue(Bytes.toBytes("cluster"), Bytes.toBytes("replyCount"));
        int replyCount = Integer.parseInt(new String(replyCountByte));
        byte[] retweetCountByte = result.getValue(Bytes.toBytes("cluster"), Bytes.toBytes("retweetCount"));
        int retweetCount = Integer.parseInt(new String(retweetCountByte));
        byte[] likeCountByte = result.getValue(Bytes.toBytes("cluster"), Bytes.toBytes("likeCount"));
        int likeCount = Integer.parseInt(new String(likeCountByte));
        byte[] timeByte = result.getValue(Bytes.toBytes("cluster"), Bytes.toBytes("time"));
        int time = Integer.parseInt(new String(timeByte));
        return new Helper(numOfHashtags, language, hasVideo, replyCount, retweetCount, likeCount, time);
    }
}
