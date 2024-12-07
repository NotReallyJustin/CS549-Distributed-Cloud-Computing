package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class FinMapper extends Mapper<LongWritable, Text, DoubleWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException, IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits each line

		/*
		 * TODO output key:-rank, value: node
		 * See IterMapper for hints on parsing the output of IterReducer.
		 */

		// Input Key: Line Number
		// Input Value: Vertex;Rank and space seperated list of adjacent vertices

		// output key: -rank (to sort in reverse order)
		// Output vertex: Vertex

		if (sections.length > 2)
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}

		context.write(new DoubleWritable(-1 * Double.parseDouble(sections[1])), new Text(sections[0]));
	}

}
