package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

	public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TextPair ensures that we have values with tag "0" first, followed by tag "1"
		 * So we know that first value is the name and second value is the rank
		 */
		String k = key.toString(); // Converts the key to a String
		
		// TODO values should have the vertex name and the page rank (in that order).
		// Emit (vertex name, pagerank) or (vertex id, vertex name, pagerank)
		// We're going to emit (vertex name, pagerank)
		// Ignore if the values do not include both vertex name and page rank

		// Input Key: Vertex ID
		// Input Value: List with vertex name and vertex rank

		// Output Key: Vertex Name
		// Output Key: Vertex Rank

		String vertexName = "";
		String vertexRank = "";

		// We don't know why values is an iterable
		// Ask Duggan what to do here

		// For now, we're treating this as a normal iterable but reduce only sends one output
		for (Text value : values)
		{
			String[] valueSplit = value.toString().split("\t");

			// Not sure what to do here, but I'll just say that if the valueSplit has vertexName and vertexRank,
			// appropriately return that

			if (valueSplit.length == 2)
			{
				vertexName = valueSplit[0].trim();
				vertexRank = valueSplit[1].trim();

				// Only one emit so
				break;
			}
		}

		if (vertexName != null && vertexRank != null)
		{
			context.write(new Text(vertexName), new Text(vertexRank));
		}
	}
}
