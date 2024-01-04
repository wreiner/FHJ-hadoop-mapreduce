package at.fhj;

import java.awt.*;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Map Class which extends MaReduce.Mapper class
 * Map is passed a single line at a time, it splits the line based on space
 * and generated the token which are output by map with value as one to be consumed
 * by reduce class
 * @author Raman
 */
public class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
	private static final Pattern LINE_PATTERN = Pattern.compile("^\\d{2}\\.(\\d{2})\\.(\\d{4});(?!9\\d+;)\\d+;.*?;(\\d+);");

	private static final Map<String, Map<String, Integer>> map = new ConcurrentHashMap<>();

	private static final IntWritable one = new IntWritable(1);
	private boolean yearlyKey;

    private Text word = new Text();

	public void setup(Context context) {
		Configuration config = context.getConfiguration();
		this.yearlyKey = config.getBoolean("yearly", false);
		System.out.println("will use year in key: " + this.yearlyKey);
	}

    /** map function of Mapper parent class takes a line of text at a time
        splits to tokens and passes to the context as word along with value as one  */

	private static Matcher parseLine(String line) {
		Matcher m = LINE_PATTERN.matcher(line);
		if (!m.find()) {
			return null;
		}

		return m;
	}

	@Override
	protected void map(LongWritable key, Text value,
			Context context)
			throws IOException, InterruptedException {

		String line = value.toString().replaceAll("[^a-zA-Z0-9\\.;]", "");
		Matcher m = parseLine(line);
		if (m == null) {
			return;
		}

		String useKey = m.group(1);
		if (this.yearlyKey) {
			useKey += "." + m.group(2);
		}

		if (!map.containsKey(useKey)) {
			map.put(useKey, new HashMap<String, Integer>());
		}

		if (map.get(useKey).containsKey(m.group(3))) {
			System.out.println("JOB " + m.group(3) + " already found in " + useKey);
			return;
		}

		map.get(useKey).put(m.group(3), 1);

		word.set(useKey);
		context.write(word, one);
	}
}