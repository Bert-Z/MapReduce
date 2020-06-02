package sjtu.sdic.mapreduce;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cachhe on 2019/4/21.
 */
public class WordCount {

    public static List<KeyValue> mapFunc(String file, String value) {
        // Your code here (Part II)
        try {
            Pattern p = Pattern.compile("[^a-zA-Z0-9]+");
            String[] words = p.split(value);

            List<KeyValue> lkv = new ArrayList<KeyValue>();
            for (String word : words)
                lkv.add(new KeyValue(word, "1"));

            return lkv;
        } catch (Exception e) {
            e.printStackTrace();
        }

        return null;
    }

    public static String reduceFunc(String key, String[] values) {
        // Your code here (Part II)
        try {
            int retNum = 0;
            for (String value : values){
                retNum += Integer.parseInt(value);
            }

            return Integer.toString(retNum);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        if (args.length < 3) {
            System.out.println("error: see usage comments in file");
        } else if (args[0].equals("master")) {
            Master mr;

            String src = args[2];
            File file = new File(".");
            String[] files = file.list(new WildcardFileFilter(src));
            if (args[1].equals("sequential")) {
                mr = Master.sequential("wcseq", files, 3, WordCount::mapFunc, WordCount::reduceFunc);
            } else {
                mr = Master.distributed("wcdis", files, 3, args[1]);
            }
            mr.mWait();
        } else {
            Worker.runWorker(args[1], args[2], WordCount::mapFunc, WordCount::reduceFunc, 100, null);
        }
    }
}
