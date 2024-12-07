package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class JoinReducer extends Reducer<TextPair, Text, Text, Text> {

	public void reduce(TextPair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		/* 
		 * TextPair ensures that we have values with tag "0" first, followed by tag "1"
		 * So we know that first value is the name and second value is the rank
		 * Not sure what we do with this key
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
		int i = 0;
		for (Text value : values)
		{
			if (i == 0)
			{
				vertexName = value.toString();
			}
			else if (i == 1)
			{
				vertexRank = value.toString();
			}

			i++;
		}

		if (vertexName != null && vertexRank != null)
		{
			context.write(new Text(vertexName), new Text(vertexRank));
		}
	}
}
