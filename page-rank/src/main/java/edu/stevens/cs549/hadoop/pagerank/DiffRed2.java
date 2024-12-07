package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed2 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double diff_max = 0.0; // sets diff_max to a default value
		/* 
		 * TODO: Compute and emit the maximum of the differences
		 */

		// Input key: "Difference"
		// Input value: List of rank differences for each vertex

		// Output key: Maximum difference between the values
		// Output value: null

		for (Text val : values)
		{
			double diff = Double.parseDouble(val.toString());

			if (diff > diff_max)
			{
				diff_max = diff;
			}
		}

		// TODO: Output
		// context.write(new Text(diff_max + ""), null);
		context.write(new Text(), new Text(diff_max + ""));
	}
}
