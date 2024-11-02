package edu.stevens.cs549.dht.main;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.DhtBase;
import edu.stevens.cs549.dht.events.EventConsumer;
import edu.stevens.cs549.dht.events.IEventListener;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceBlockingStub;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceStub;
import edu.stevens.cs549.dht.state.IChannels;
import edu.stevens.cs549.dht.state.IState;
import io.grpc.Channel;
import io.grpc.stub.StreamObserver;

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
	private DhtServiceBlockingStub getStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newBlockingStub(channel);
	}

	private DhtServiceBlockingStub getStub(NodeInfo target) throws DhtBase.Failed {
		return getStub(target.getHost(), target.getPort());
	}

	private DhtServiceStub getListenerStub(String targetHost, int targetPort) throws DhtBase.Failed {
		Channel channel = channels.getChannel(targetHost, targetPort);
		return DhtServiceGrpc.newStub(channel);
	}

	private DhtServiceStub getListenerStub(NodeInfo target) throws DhtBase.Failed {
		return getListenerStub(target.getHost(), target.getPort());
	}


	/*
	 * TODO: Fill in missing operations.
	 */
	// Get info about a given node
	public NodeInfo getNodeInfo(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "getNodeInfo("+node.getId()+")");

		return this.getStub(node).getNodeInfo(Empty.getDefaultInstance());
	}

	/*
	 * Get the predecessor pointer at a node.
	 */
	public OptNodeInfo getPred(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "s("+node.getId()+")");
		return getStub(node).getPred(Empty.getDefaultInstance());
	}

	/*
	 * Get the predecessor pointer at a node.
	 */
	public NodeInfo getSucc(NodeInfo node) throws DhtBase.Failed {
		Log.weblog(TAG, "getSucc("+node.getId()+")");

		return this.getStub(node).getSucc(Empty.getDefaultInstance());
	}

	// I know the actual call uses the RPC Id object
	// But my friend suggested that things are much easier if you convert them in WebClient
	// instead of trying to fiddle in DHT so I'm just going to copy the function
	// parameters in Dht.java
	public NodeInfo closestPrecedingFinger(NodeInfo node, int id) throws DhtBase.Failed {
		Log.weblog(TAG, "closestPrecedingFinger("+node.getId()+", " + id + ")");

		// We need to build an ID because gRPC request
		Id.Builder builder = Id.newBuilder();
		builder.setId(id);
		Id idWrapper= builder.build();

		return this.getStub(node).closestPrecedingFinger(idWrapper);
	}

	public Bindings getBindings(NodeInfo node, String key) throws DhtBase.Failed
	{
		Log.weblog(TAG, "getBindings("+node.getId()+", " + key + ")");

		Key.Builder builder = Key.newBuilder();
		builder.setKey(key);
		Key keyStub = builder.build();

		// Remember to convert this back in DHT
		return this.getStub(node).getBindings(keyStub);
	}

	// Remember that add() in DHT is different here - build + convert later
	public Empty addBinding(NodeInfo node, String key, String value) throws DhtBase.Failed
	{
		Log.weblog(TAG, "addBinding("+node.getId() + ", " + key + ", " + value + ")");

		Binding.Builder builder = Binding.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		Binding bindingStub = builder.build();

		return this.getStub(node).addBinding(bindingStub);
	}

	// Remember that delete() in DHT is different here - build + convert later
	public Empty deleteBinding(NodeInfo node, String key, String value) throws DhtBase.Failed
	{
		Log.weblog(TAG, "deleteBinding(" + node.getId() + ", " + key + ", " + value + ")");

		// Just copy what we have in addBinding
		Binding.Builder builder = Binding.newBuilder();
		builder.setKey(key);
		builder.setValue(value);
		Binding bindingStub = builder.build();

		return this.getStub(node).deleteBinding(bindingStub);
	}

	public NodeInfo findSuccessor(NodeInfo node, int id) throws DhtBase.Failed {
//		System.out.println("Succ Called!");
		Log.weblog(TAG, "deleteBinding(" + node.getId() + ", " + id + ")");

		Id.Builder builder = Id.newBuilder();
		builder.setId(id);
		Id actualID = builder.build();

		return this.getStub(node).findSuccessor(actualID);
	}

	/*
	 * Notify node that we (think we) are its predecessor.
	 */
	public OptNodeBindings notify(NodeInfo node, NodeBindings predDb) throws DhtBase.Failed {
		Log.weblog(TAG, "notify("+node.getId()+")");
		// TODO

		/*
		 * The protocol here is more complex than for other operations. We
		 * notify a new successor that we are its predecessor, and expect its
		 * bindings as a result. But if it fails to accept us as its predecessor
		 * (someone else has become intermediate predecessor since we found out
		 * this node is our successor i.e. race condition that we don't try to
		 * avoid because to do so is infeasible), it notifies us by returning
		 * null. This is represented in HTTP by RC=304 (Not Modified).
		 */
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

	/*
	 * Listening for new bindings.
	 */
	public void listenOn(NodeInfo node, Subscription subscription, IEventListener listener) throws DhtBase.Failed {
		Log.weblog(TAG, "listenOn("+node.getId()+")");
		// TODO listen for updates for the key specified in the subscription ;-;

		// We need a Stream observer object that can call the appropriate clientside RPC functions
		// based on what's happening from the server

		// NOTE: WE FETCH IN NEW KEYS
		// SUBSCRIPTION HAS CURRENT KEYS
		StreamObserver<Event> observer = new StreamObserver<>()
		{
			@Override
			public void onNext(Event event)
			{
				// If we get a binding event, it's either a new binding or moved binding
				// Handle that appropriately
				if (event.hasNewBinding())
				{
					Binding binding = event.getNewBinding();
					listener.onNewBinding(binding.getKey(), binding.getValue());
				}
				else
				{
					listener.onMovedBinding(subscription.getKey());
				}
			}

			@Override
			public void onError(Throwable throwable)
			{
				// I think we just call onError on current key
				listener.onError(subscription.getKey(), throwable);
			}

			@Override
			public void onCompleted()
			{
				// Close current key
				// Autocomplete by Intellij
				listener.onClosed(subscription.getKey());
			}
		};

		this.getListenerStub(node).listenOn(subscription, observer);
		// Do we do smth with listener
	}

	public void listenOff(NodeInfo node, Subscription subscription) throws DhtBase.Failed {
		Log.weblog(TAG, "listenOff("+node.getId()+")");

		// TODO stop listening for updates on bindings to the key in the subscription
//		this.getStub(node).listenOff(subscription);

		// Praying this works because I'm not sure what to put for null subscription
		// When I do a listenerstub, this returns null. So we're only using a blocking stub
		System.out.println("Making RPC call for listenOff on " + subscription.getKey() + " and " + subscription.getId());
		this.getStub(node).listenOff(subscription);
	}
	
}
