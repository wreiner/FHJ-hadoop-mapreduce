package at.fhj;

import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reduce class which is executed after the map class and takes
 * key(word) and corresponding values, sums all the values and write the
 * word along with the corresponding total occurrences in the output
 * 
 * @author Raman
 */
public class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable>{

	SortedMap<Text, Integer> jobs = Collections.synchronizedSortedMap(new TreeMap<>());

	/** Method which performs the reduce operation and sums
	    all the occurrences of the word before passing it to be stored in output */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context)
			throws IOException, InterruptedException {
	
		int sum = 0;
		Iterator<IntWritable> valuesIt = values.iterator();
		/** add next value to that key (in that case always 1, which we set as value in the map class */
		while(valuesIt.hasNext()){
			sum = sum + valuesIt.next().get();
		}
		
		// context.write(key, new IntWritable(sum));
		jobs.put(new Text(key), sum);
	}

	@Override
	protected void cleanup(Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		Map<Text, Integer> sortedMap = sortByValues(jobs);
		for (Map.Entry<Text, Integer> jobs : sortedMap.entrySet()) {
			context.write(jobs.getKey(), new IntWritable(jobs.getValue()));
		}
	}

	public static <K, V extends Comparable<V>> Map<K, V>
	sortByValues(final Map<K, V> map) {
		Comparator<K> valueComparator =
				new Comparator<K>() {
					public int compare(K k1, K k2) {
						int compare =
								map.get(k2).compareTo(map.get(k1));
						if (compare == 0)
							return 1;
						else
							return compare;
					}
				};

		Map<K, V> sortedByValues =
				new TreeMap<K, V>(valueComparator);
		sortedByValues.putAll(map);
		return sortedByValues;
	}


}