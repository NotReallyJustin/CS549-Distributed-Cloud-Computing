package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class InitMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		/* 
		 * TODO: Just echo the input, since it is already in adjacency list format.
		 * Alternatively, output adjacency pairs that will be collected by reducer.
		 */

		// Input is NodeID and space delimited adjacency list split by :
		// We probably want to trim the second list in case space delimeters go wrong

		String[] divider = line.split(":");
		context.write(new Text(divider[0].trim()), new Text(divider[1].trim()));

	}

}
