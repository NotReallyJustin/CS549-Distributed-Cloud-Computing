package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap2 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String s = value.toString(); // Converts Line to a String
		String[] sections = s.split("\t"); // Splits each line

		/* 
		 * TODO: emit: key:"Difference" value:difference calculated in DiffRed1
		 */

		// Input key: Line # (no one cares)
		// Input value: Rank difference

		// Output key: "Difference"
		// Input key: Difference value as text

		context.write(new Text("Difference"), new Text(sections[1]));
	}

}
