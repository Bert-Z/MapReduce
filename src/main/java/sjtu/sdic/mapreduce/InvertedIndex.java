package sjtu.sdic.mapreduce;

import org.apache.commons.io.filefilter.WildcardFileFilter;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.core.Master;
import sjtu.sdic.mapreduce.core.Worker;

import java.io.File;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Cachhe on 2019/4/24.
 */
public class InvertedIndex {

    public static List<KeyValue> mapFunc(String file, String value) {
        // Your code here (Part V)
        try {
            Pattern p = Pattern.compile("[^a-zA-Z0-9]+");
            String[] words = p.split(value);

            List<KeyValue> lkv = new ArrayList<KeyValue>();
            for (String word : words)
                lkv.add(new KeyValue(word, file));

            return lkv;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static String reduceFunc(String key, String[] values) {
        //  Your code here (Part V)
        try {
            Set<String> set = new HashSet<String>();
            for (String value : values) {
                set.add(value);
            }
            String ret = Integer.toString(set.size())+" ";
            for (String s : set) {
                ret += (s + ",");
            }
            return ret.substring(0, ret.length()-1);
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
                mr = Master.sequential("iiseq", files, 3, InvertedIndex::mapFunc, InvertedIndex::reduceFunc);
            } else {
                mr = Master.distributed("wcdis", files, 3, args[1]);
            }
            mr.mWait();
        } else {
            Worker.runWorker(args[1], args[2], InvertedIndex::mapFunc, InvertedIndex::reduceFunc, 100, null);
        }
    }
}
