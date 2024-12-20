package edu.stevens.cs549.dht.server;

import com.google.protobuf.Empty;
import edu.stevens.cs549.dht.activity.Dht;
import edu.stevens.cs549.dht.activity.DhtBase.Failed;
import edu.stevens.cs549.dht.activity.DhtBase.Invalid;
import edu.stevens.cs549.dht.activity.IDhtNode;
import edu.stevens.cs549.dht.activity.IDhtService;
import edu.stevens.cs549.dht.events.EventProducer;
import edu.stevens.cs549.dht.main.Log;
import edu.stevens.cs549.dht.rpc.*;
import edu.stevens.cs549.dht.rpc.DhtServiceGrpc.DhtServiceImplBase;
import edu.stevens.cs549.dht.rpc.NodeInfo;
import io.grpc.stub.StreamObserver;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/*
 * Additional resource logic.  The Web resource operations call
 * into wrapper operations here.  The main thing these operations do
 * is to call into the DHT service object, and wrap internal exceptions
 * as HTTP response codes (throwing WebApplicationException where necessary).
 * 
 * This should be merged into NodeResource, then that would be the only
 * place in the app where server-side is dependent on JAX-RS.
 * Client dependencies are in WebClient.
 * 
 * The activity (business) logic is in the dht object, which exposes
 * the IDHTResource interface to the Web service.
 */

public class NodeService extends DhtServiceImplBase {
	
	private static final String TAG = NodeService.class.getCanonicalName();
	
	private static Logger logger = Logger.getLogger(TAG);

	/**
	 * Each service request is processed by a distinct service object.
	 *
	 * Shared state is in the state object; we use the singleton pattern to make sure it is shared.
	 */
	private IDhtService getDht() {
		return Dht.getDht();
	}
	
	// TODO: add the missing operations

	private void error(String mesg, Exception e) {
		logger.log(Level.SEVERE, mesg, e);
	}

	@Override
	public void getNodeInfo(Empty empty, StreamObserver<NodeInfo> responseObserver) {
		Log.weblog(TAG, "getNodeInfo()");
		responseObserver.onNext(getDht().getNodeInfo());
		responseObserver.onCompleted();
	}
	// Note to self: Continue here
	// Copy template of getNodeInfo()
	@Override
	public void getPred(Empty empty, StreamObserver<OptNodeInfo> responseObserver)
	{
		Log.weblog(TAG, "getPred()");

		// It's weird because we have OptNodeInfo but then there's a *
		// Dunno what to do with them
		OptNodeInfo pred = this.getDht().getPred();
		responseObserver.onNext(pred);
		responseObserver.onCompleted();
	}

	@Override
	public void getSucc(Empty request, StreamObserver<NodeInfo> responseObserver)
	{
		Log.weblog(TAG, "getSucc()");

		NodeInfo succ = this.getDht().getSucc();
		responseObserver.onNext(succ);
		responseObserver.onCompleted();
	}

	@Override
	public void closestPrecedingFinger(Id requestID, StreamObserver<NodeInfo> responseObserver)
	{
		Log.weblog(TAG, "closestPrecedingFinger()");

		// Get the closest preceding finger of what's in ID
		NodeInfo id = this.getDht().closestPrecedingFinger(requestID.getId());
		responseObserver.onNext(id);
		responseObserver.onCompleted();
	}

	@Override
	public void notify(NodeBindings request, StreamObserver<OptNodeBindings> responseObserver)
	{
		Log.weblog(TAG, "notify()");

		// notify is a DHT function so we're literally just going to copy and paste what we have
		OptNodeBindings nodeBind = this.getDht().notify(request);
		responseObserver.onNext(nodeBind);
		responseObserver.onCompleted();
	}

	@Override
	public void getBindings(Key key, StreamObserver<Bindings> responseObserver)
	{
		Log.weblog(TAG, "getBindings()");

		// You need to physically get the key
		String keyStr = key.getKey();

		// https://www.cs.cornell.edu/courses/cs3110/2017fa/l/15-hashtable/notes.html
		// According to Cornell, a binding for a key is just the value.
		// Guess a DHT binding is just all the values under the node, but in a Bindings format
		// Note from class notes: you need to use the builder when you build a gRPC object
		// Oh look intellij AI did this for me
		Bindings.Builder builder = Bindings.newBuilder();
		builder.setKey(keyStr);

		// Apparently get() throws an exception we have to handle
		try
		{
			String[] values = this.getDht().get(keyStr);

			// I can't get builder.addAllValue(values) to work so we're doing it one by one ig
			for (int i = 0; i < values.length; i++)
			{
				builder.addValue(values[i]);
			}

			Bindings binding = builder.build();
			responseObserver.onNext(binding);
			responseObserver.onCompleted();
		}
		catch(Exception e)
		{
			this.error(e.getMessage(), e);
		}
	}

	@Override
	public void addBinding(Binding binding, StreamObserver<Empty> responseObserver)
	{
		Log.weblog(TAG, "addBinding()");

		// Try to generate a key-value binding thingy :think:
		// I think we can just call add function in dht
		// TO DO: Check this

		// Also this throws an exception so handle that
		try
		{
			this.getDht().add(binding.getKey(), binding.getValue());

			// Auto-generated by intellij intellisense so I assume this works
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		}
		catch(Exception e)
		{
			this.error(e.getMessage(), e);
		}
	}

	@Override
	public void deleteBinding(Binding binding, StreamObserver<Empty> responseObserver)
	{
		Log.weblog(TAG, "deleteBinding()");

		// Apparently delete *also* throws an exception
		// Damn let's handle that
		try
		{
			this.getDht().delete(binding.getKey(), binding.getValue());

			// Intellij Intellisense auto generated
			responseObserver.onNext(Empty.getDefaultInstance());
			responseObserver.onCompleted();
		}
		catch (Exception e)
		{
			this.error(e.getMessage(), e);
		}
	}

	@Override
	public void findSuccessor(Id requestID, StreamObserver<NodeInfo> responseObserver)
	{
		Log.weblog(TAG, "findSuccessor()");

		// Call find successor function in DHT for DHT nodes
		// Apparently findSuccessor *also* throws an exception bruh
		try
		{
			NodeInfo id = this.getDht().findSuccessor(requestID.getId());
			responseObserver.onNext(id);
			responseObserver.onCompleted();
		}
		catch(Exception e)
		{
			this.error(e.getMessage(), e);
		}
	}

	// The server should still handle listenOn and listenOff
	@Override
	public void listenOn(Subscription subscription, StreamObserver<Event> responseObserver)
	{
		Log.weblog(TAG, "listenOn()");

		try
		{
			// Suggested by IntelliJ autocomplete
			EventProducer eventProducer = EventProducer.create(responseObserver);

			this.getDht().listenOn(subscription.getId(), subscription.getKey(), eventProducer);
		}
		catch(Exception e)
		{
			responseObserver.onError(e);
		}
	}


	@Override
	public void listenOff(Subscription subscription, StreamObserver<Empty> responseObserver){
		Log.weblog(TAG, "listenOff()");

		try
		{
			this.getDht().listenOff(subscription.getId(), subscription.getKey());

			// Close the listening channel I think
			responseObserver.onNext(Empty.newBuilder().build());
			responseObserver.onCompleted();
		}
		catch (Exception e)
		{
			responseObserver.onError(e);
			throw new RuntimeException(e);
		}
	}

}