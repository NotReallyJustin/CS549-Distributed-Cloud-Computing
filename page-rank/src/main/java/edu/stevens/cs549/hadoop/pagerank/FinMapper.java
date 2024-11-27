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

		String[] vertexRankSplit = sections[0].split(";");
		String vertex = vertexRankSplit[0];
		String rank = vertexRankSplit[1];
		int negativeRank = -1 * Integer.parseInt(rank);

		// TODO: Write
		// IDK why but IntelliJ is telling me to make this a DoubleWritable type
		// It won't let me do string conversion either
		context.write(new DoubleWritable(negativeRank), new Text(vertex));
	}

}
