package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffMap1 extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits each line
		if (sections.length > 2) // checks for incorrect data format
		{
			throw new IOException("Incorrect data format");
		}
		/**
		 *  TODO: read node-rank pair and emit: key:node, value:rank
		 */

		// Input Key: Line Number (Ignored)
		// Input Value 1: vertex;rank
		// Input Value 2: Space delimited list of adjacent vertices

		// Output Key: Node
		// Output Value: Rank

		// I think we just parse input value 1 and ignore input value 2
		String[] delinParse = sections[0].split(";");
		context.write(new Text(delinParse[0]), new Text(delinParse[1]));
	}

}
