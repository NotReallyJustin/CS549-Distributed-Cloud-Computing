package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitReducer extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: Output key: node+rank, value: adjacency list
		 */

		// Input Key is the vertex
		// The input value is an array of adjacency lists

		// Output key is vertex;rank (1 to begin with)
		// Output value is the space seperated list of vertices
		// The hint wanted this to be comma delimited
		// but I didn't want to mess with it since it's already space delimited and it doesn't do much in the long run

		for (Text adjacencyList : values)
		{
			context.write(new Text((key + ";1").trim()), adjacencyList);
		}
	}
}
