package edu.stevens.cs549.hadoop.pagerank;

import java.io.IOException;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterMapper extends Mapper<LongWritable, Text, Text, Text> {

	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException,
			IllegalArgumentException {
		String line = value.toString(); // Converts Line to a String
		String[] sections = line.split("\t"); // Splits it into two parts. Part 1: node;rank | Part 2: adj list

		if (sections.length > 2) // Checks if the data is in the incorrect format
		{
			throw new IOException("Incorrect data format");
		}
		if (sections.length != 2) {
			return;
		}
		
		/* 
		 * TODO: emit key: adj vertex, value: computed weight.
		 * 
		 * Remember to also emit the input adjacency list for this node!
		 * Put a marker on the string value to indicate it is an adjacency list.
		 */

		// Input key: NodeID;rank
		// Input value: space delimited adjacency list

		// Output 1 key: Adjacent vertex (item in adjacency vertex)
		// Output 1 value: Computed weights
		// 🚨 I tried outputting normal weights and then calculating the computed weights in the IterReducer
		// 🚨 But the math broke and I don't know why so we're doing it in IterMapper

		// Output 2 key: NodeID
		// Output 2 value: @ + Space delimited adjacency list (instead of stuff in back)

		String[] nodeSplit = sections[0].split(";");
		String nodeID = nodeSplit[0];
		String nodeWeight = nodeSplit[1];

		String adjacencyListStr = sections[1];
		String[] adjacencyList = sections[1].split(" ");

		// Calculate equal weight split
		// The below is suggested by Intellij intellisense
		double weight = Double.parseDouble(nodeWeight);
		double equalWeight = weight / adjacencyList.length;

		// For all adjacent vertex, write the equal weight
		for (String adjacentVertex : adjacencyList)
		{
			context.write(new Text(adjacentVertex), new Text("" + equalWeight));
		}

		// Write NodeID and @ + space delimited adjacency list
		context.write(new Text(nodeID), new Text("@" + adjacencyListStr));
	}

}
