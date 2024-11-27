package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class IterReducer extends Reducer<Text, Text, Text, Text> {
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TODO: emit key:node+rank, value: adjacency list
		 * Use PageRank algorithm to compute rank from weights contributed by incoming edges.
		 * Remember that one of the values will be marked as the adjacency list for the node.
		 */
		double d = PageRankDriver.DECAY; // Decay factor
		double rank = 0.0; // stores the decay factor in a variable rank

		// Input Key: Vertex ID
		// Input Value: Array with [Adjacent Vertices (w/ equal weights) and space-seperated adjacency lists]

		// Output Key: VertexID;Rank (Compute Rank from weights)
		// Output Value: Space-Seperated adjacency list

		// Declare some variables we'll process later
		String nodeID = key.toString();
		String adjacencyList = "";

		for (Text inputVal : values)
		{
			String valueStr = inputVal.toString();
			if (valueStr.startsWith("@"))
			{
				// Here we have space-seperated adjacency list
				// Remove le delimiter here
				adjacencyList = valueStr.replaceAll("@", "");
			}
			else
			{
				// Here we have equal weights of adj vertices
				rank += Double.parseDouble(valueStr);
			}
		}

		// Process decay factor acc to formula
		rank = rank * d;
		rank += (1 - d);

		// Emit reducer
		context.write(new Text(nodeID + ";" + rank), new Text(adjacencyList));
	}
}
