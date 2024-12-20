package edu.stevens.cs549.dht.activity;

import edu.stevens.cs549.dht.rpc.NodeInfo;
import edu.stevens.cs549.dht.state.IRouting;

public abstract class DhtBase {
	
	public static int NodeKey(String k) {
		/*
		 * See http://www.javablogging.com/a-reminder-about-remainder-operator/
		 */
		return Math.abs(k.hashCode() % IRouting.NKEYS);
	}
	
	/*
	 * Web service failure is mapped to this exception.
	 */
	public static class Failed extends Exception { 
		private static final long serialVersionUID = 1L;
		public Failed(String s) { super(s); }
		public Failed() { super("Simulated failure."); }
	}
	
	/*
	 * For internal errors, unlike Failed.
	 * Also encapsulate RemoteException errors.
	 */
	public static class Error extends Exception { 
		private static final long serialVersionUID = 1L;
		public Error(Exception e) { super(e); }
		public Error(String s) { super(s); }
	}
	
	/*
	 * For input validation errors.
	 */
	public static class Invalid extends Exception {
		private static final long serialVersionUID = 1L;
		public Invalid(String s) { super(s); }
	}

	/*
	 * Check if the id is in a range of key values.  LB is excluded,
	 * and UB is only included if the flag is true.
	 * i.e. default case for checking if id is in range for node.
	 */
	public static boolean inInterval(int id, int LB, int UB, boolean includeUB) {
		if (includeUB && id==UB) 
			return true;
		if (LB < UB) {
			UB = (UB - LB);
			id = (id - LB);
			return 0 < id && id < UB;
		} else if (UB < LB) {
			UB = (UB + (IRouting.NKEYS - LB)) % IRouting.NKEYS;
			id = (id + (IRouting.NKEYS - LB)) % IRouting.NKEYS;
			return 0 < id && id < UB;
		} else {
			return false;
		}
	}
	
	public static boolean inInterval(int id, int LB, int UB) {
		return inInterval(id, LB, UB, true);
	}

	public static boolean isEqual(NodeInfo n1, NodeInfo n2) {
		if (n1 == null) {
			throw new IllegalArgumentException("isEqual: first argument is null!");
		} else if (n2 == null) {
			throw new IllegalArgumentException("isEqual: second argument is null!");
		} else {
			return n1.getHost().equals(n2.getHost()) && n1.getPort()==n2.getPort();
		}
	}
}
