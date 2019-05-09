package sjtu.sdic.mapreduce.core;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import sjtu.sdic.mapreduce.common.KeyValue;
import sjtu.sdic.mapreduce.common.Utils;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created by Cachhe on 2019/4/19.
 */
public class Reducer {

    /**
     * 
     * 	doReduce manages one reduce task: it should read the intermediate
     * 	files for the task, sort the intermediate key/value pairs by key,
     * 	call the user-defined reduce function {@code reduceFunc} for each key,
     * 	and write reduceFunc's output to disk.
     * 	
     * 	You'll need to read one intermediate file from each map task;
     * 	{@code reduceName(jobName, m, reduceTask)} yields the file
     * 	name from map task m.
     *
     * 	Your {@code doMap()} encoded the key/value pairs in the intermediate
     * 	files, so you will need to decode them. If you used JSON, you can refer
     * 	to related docs to know how to decode.
     * 	
     *  In the original paper, sorting is optional but helpful. Here you are
     *  also required to do sorting. Lib is allowed.
     * 	
     * 	{@code reduceFunc()} is the application's reduce function. You should
     * 	call it once per distinct key, with a slice of all the values
     * 	for that key. {@code reduceFunc()} returns the reduced value for that
     * 	key.
     * 	
     * 	You should write the reduce output as JSON encoded KeyValue
     * 	objects to the file named outFile. We require you to use JSON
     * 	because that is what the merger than combines the output
     * 	from all the reduce tasks expects. There is nothing special about
     * 	JSON -- it is just the marshalling format we chose to use.
     * 	
     * 	Your code here (Part I).
     * 	
     * 	
     * @param jobName the name of the whole MapReduce job
     * @param reduceTask which reduce task this is
     * @param outFile write the output here
     * @param nMap the number of map tasks that were run ("M" in the paper)
     * @param reduceFunc user-defined reduce function
     */
    public static void doReduce(String jobName, int reduceTask, String outFile, int nMap, ReduceFunc reduceFunc) {
        List<String> keys = new ArrayList<>();
        TreeMap<String, List<String>> map = new TreeMap<>();
        try {
            for (int i = 0; i < nMap; i++) {
                File file = new File(Utils.reduceName(jobName, i, reduceTask));
                Long length = file.length();
                char[] content = new char[length.intValue()];
                FileReader fr = new FileReader(file);
                fr.read(content);
                fr.close();
                file.delete();
                JSONArray jsonArray = JSONArray.parseArray(String.valueOf(content));
                List<KeyValue> list = JSONObject.parseArray(jsonArray.toJSONString(), KeyValue.class);
                for (int j = 0; j < list.size(); j++) {
                    if (!keys.contains(list.get(j).key)) {
                        keys.add(list.get(j).key);
                        map.put(list.get(j).key, new ArrayList<>());
                    }
                    List<String> values = map.get(list.get(j).key);
                    values.add(list.get(j).value);
                    map.put(list.get(j).key, values);
                }
            }

            JSONObject jsonObject = new JSONObject();
            for (int i = 0; i < keys.size(); i++) {
                String key = keys.get(i);
                List<String> l = map.get(key);
                String[] values = l.toArray(new String[l.size()]);
                String value = reduceFunc.reduce(key, values);
                jsonObject.put(key, value);
            }

            FileWriter fw = new FileWriter(outFile);
            fw.write(jsonObject.toJSONString());
            fw.flush();
            fw.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
