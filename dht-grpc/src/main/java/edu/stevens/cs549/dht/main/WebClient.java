package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.state.IChannels;
import edu.stevens.cs549.dht.state.IState;
import io.grpc.Channel;

import java.util.logging.Level;
import java.util.logging.Logger;

public class WebClient {
	
	private static final String TAG = WebClient.class.getCanonicalName();

	private Logger logger = Logger.getLogger(TAG);

	private IChannels channels;

	private WebClient(IChannels channels) {
		this.channels = channels;
	}

	public static WebClient getInstance(IState state) {
		return new WebClient(state.getChannels());
	}

	private void error(String msg, Exception e) {
		logger.log(Level.SEVERE, msg, e);
	}

	private void info(String mesg) {
		Log.weblog(TAG, mesg);
	}

	/*
	 * Get a blocking stub (channels and stubs are cached for reuse).
	 */
	private DhtServiceGrpc.DhtServiceBlockingStub getStub(String targetHost, int targetPort) {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newBlockingStub(channel);
	}

	private DhtServiceGrpc.DhtServiceBlockingStub getStub(NodeInfo target) {
		return getStub(target.getHost(), target.getPort());
	}


	/*
	 * TODO: Fill in missing operations.
	 *  Justin Note: We'll copy the syntax provided by getPred()
	 */

	// Get info about a given node
	public NodeInfo getNodeInfo(NodeInfo node) {
		Log.weblog(TAG, "getNodeInfo("+node.getId()+")");

		return this.getStub(node).getNodeInfo(Empty.getDefaultInstance());
	}

	/*
	 * Get the predecessor pointer at a node.
	 */
	public OptNodeInfo getPred(NodeInfo node) {
		Log.weblog(TAG, "getPred("+node.getId()+")");
		return getStub(node).getPred(Empty.getDefaultInstance());
	}

	/*
	 * Get the predecessor pointer at a node.
	 */
	public NodeInfo getSucc(NodeInfo node) {
		Log.weblog(TAG, "getSucc("+node.getId()+")");

		return this.getStub(node).getSucc(Empty.getDefaultInstance());
	}

	// I know the actual call uses the RPC Id object
	// But my friend suggested that things are much easier if you convert them in WebClient
	// instead of trying to fiddle in DHT so I'm just going to copy the function
	// parameters in Dht.java
	public NodeInfo closestPrecedingFinger(NodeInfo node, int id) {
		Log.weblog(TAG, "closestPrecedingFinger("+node.getId()+", " + id + ")");

		// We need to build an ID because gRPC request
		Id.Builder builder = Id.newBuilder();
		builder.setId(id);
		Id idWrapper= builder.build();

		return this.getStub(node).closestPrecedingFinger(idWrapper);
	}

	/*
	 * Notify node that we (think we) are its predecessor.
	 * node --> The successor node (according to DHT)
	 * predDb --> Bindings of current node
	 */
	public OptNodeBindings notify(NodeInfo node, NodeBindings predDb) throws DhtBase.Failed {
		// TODO
		// throw new IllegalStateException("notify() not yet implemented");
		/*
		 * The protocol here is more complex than for other operations. We
		 * *notify a new successor that we are its predecessor*, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null.
		 */
		Log.weblog(TAG, "notify("+node.getId()+", " + predDb.getInfo().getId() + ")");
//
//		NodeInfo currentNode = predDb.getInfo();
//
//		// When we try to say that we are the pred, I think we just give them our Node Bindings
//		// Then, it'll take care of it
//		NodeBindings.Builder builder = NodeBindings.newBuilder();
//		builder.setInfo(currentNode);
//		builder.setSucc(node);
//		NodeBindings currBinding = builder.build();

		// The comment blocks above makes me think that this stuff might throw an error
		// So we're gonna wrap it in try and have it return null if the notify throws an error
		try
		{
			return this.getStub(node).notify(predDb);
		}
		catch (Exception e)
		{
			this.error(e.getMessage(), e);
			return null;
		}
	}

	public Bindings getBindings(NodeInfo node, String key)
	{
		Log.weblog(TAG, "getBindings("+node.getId()+", " + key + ")");

		Key.Builder builder = Key.newBuilder();
		builder.setKey(key);
		Key keyStub = builder.build();

		// Remember to convert this back in DHT
		return this.getStub(node).getBindings(keyStub);
	}

	// Remember that add() in DHT is different here - build + convert later
	public Empty addBinding(NodeInfo node, String key, String value)
	{
		Log.weblog(TAG, "addBinding("+node.getId() + ", " + key + ", " + value + ")");

		Binding.Builder builder = Binding.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		Binding bindingStub = builder.build();

		return this.getStub(node).addBinding(bindingStub);
	}

	// Remember that delete() in DHT is different here - build + convert later
	public Empty deleteBinding(NodeInfo node, String key, String value)
	{
		Log.weblog(TAG, "deleteBinding(" + node.getId() + ", " + key + ", " + value + ")");

		// Just copy what we have in addBinding
		Binding.Builder builder = Binding.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		Binding bindingStub = builder.build();

		return this.getStub(node).deleteBinding(bindingStub);
	}

	public NodeInfo findSuccessor(NodeInfo node, int id)
	{
//		System.out.println("Succ Called!");
		Log.weblog(TAG, "deleteBinding("+node.getId() + ", " + id + ")");

		Id.Builder builder = Id.newBuilder();
		builder.setId(id);
		Id actualID = builder.build();

		return this.getStub(node).findSuccessor(actualID);
	}
}
