package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		/* 
		 * TODO: The list of values should contain two ranks.  Compute and output their difference.
		 */

		// Input Key: Vertex
		// Input Value: List with two rank values for a vertex

		// Output Key: Difference between ranks
		// Output Value: Reduce

		// Chuck the values into a ranks array
		int i = 0;
		for (Text value : values)
		{
			if (i < 2)
			{
				ranks[i] = Double.parseDouble(value.toString());
				i += 1;
			}
			else
			{
				throw new IOException("Input size of values for DiffRed1 must be 2.");
			}
		}

		// Calculate the absolute value of rank differences
		double absDiff = Math.abs(ranks[0] - ranks[1]);

		// TODO: Output
		context.write(new Text(Double.toString(absDiff)), null);
	}
}
