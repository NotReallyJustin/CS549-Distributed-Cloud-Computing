package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FinReducer extends Reducer<DoubleWritable, Text, Text, Text> {
	
//	@Override
//	public void setup(Context context) {
//		try {
//			super.setup(context);
//			URI[] files = context.getCacheFiles();
//		} catch (IOException | InterruptedException e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}
//	}

	public void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException,
			InterruptedException {
		/* 
		 * TODO: For each value, emit: key:value, value:-rank
		 */

		// Input key: Negative Page Rank (-rank)
		// Input value: List of vertices with that page rank

		// Output Key: Vertex
		// Output Value: -rank for each vertex in the list

		for (Text value : values)
		{
			context.write(new Text(value.toString()), new Text("" + (-1 * key.get())));
		}
	}
}
