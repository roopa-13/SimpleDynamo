package edu.buffalo.cse.cse486586.simpledynamo;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Formatter;
import java.util.List;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.sqlite.SQLiteDatabase;
import android.net.Uri;
import android.os.AsyncTask;
import android.telephony.TelephonyManager;
import android.util.Log;

import org.w3c.dom.Node;

import static edu.buffalo.cse.cse486586.simpledynamo.SimpleDynamoDbHelper.TABLE_NAME;

public class SimpleDynamoProvider extends ContentProvider {


	static String REMOTE_PORT0 = "11108";
	static final String REMOTE_PORT1 = "11112";
	static final String REMOTE_PORT2 = "11116";
	static final String REMOTE_PORT3 = "11120";
	static final String REMOTE_PORT4 = "11124";
	private static final int SERVER_PORT = 10000;

	List<String> portList = Arrays.asList(REMOTE_PORT0, REMOTE_PORT1, REMOTE_PORT2, REMOTE_PORT3, REMOTE_PORT4);
	private ArrayList<String> nodeList = new ArrayList<String>();
	private String[] sortedMembershipList; //111**

	private String portStr = null;
	private String myPort = null;

	ArrayList<String> node_join_list = new ArrayList<String>(5);

	private enum NodeState{INSERT, QUERYALL, QUERYKEY, DELETEALL, DELETEKEY, RECOVERY, RECOVERDATA, NEW_NODE, TEST};
	private  NodeState nodeState;

	private static final String TAG = SimpleDynamoProvider.class.getName();

	private SimpleDynamoDbHelper simpleDynamoDbHelper;
	public static final Uri BASE_URI = buildUri("content", "edu.buffalo.cse.cse486586.simpledynamo.provider");

	private static Uri buildUri(String scheme, String authority) {
		Uri.Builder uriBuilder = new Uri.Builder();
		uriBuilder.authority(authority);
		uriBuilder.scheme(scheme);
		return uriBuilder.build();
	}

	@Override
	public boolean onCreate() {
		// TODO Auto-generated method stub
		Context context = this.getContext();
		simpleDynamoDbHelper = new SimpleDynamoDbHelper(context);

		TelephonyManager tel =  (TelephonyManager) context.getSystemService(Context.TELEPHONY_SERVICE);
		portStr = tel.getLine1Number().substring(tel.getLine1Number().length() - 4);
		myPort = String.valueOf(Integer.parseInt(portStr) * 2);

		boolean flag_failure = true;
		try {
			ServerSocket serverSocket = new ServerSocket(SERVER_PORT);
			new ServerTask().executeOnExecutor(AsyncTask.THREAD_POOL_EXECUTOR, serverSocket);

			sortedMembershipList = new String[5];
			sortedMembershipList[0] = "11124";
			sortedMembershipList[1] = "11112";
			sortedMembershipList[2] = "11108";
			sortedMembershipList[3] = "11116";
			sortedMembershipList[4] = "11120";

			//checkNodeRecover();
			new ClientTask().executeOnExecutor(AsyncTask.SERIAL_EXECUTOR, myPort);

		} catch (IOException e) {
			e.printStackTrace();
		}

		return true;
	}

	private class ServerTask extends AsyncTask<ServerSocket, String, Void> {


		@Override
		protected Void doInBackground(ServerSocket... sockets) {
			ServerSocket clientSocket = sockets[0];
			Socket socket;
			DataInputStream in;
			DataOutputStream out;

			//Common variables
			String initialNode;
			String key = null;
			String value = null;


			while (true) {
				try {
					socket = clientSocket.accept();

					in = new DataInputStream(socket.getInputStream());
					String msg = in.readUTF().trim();
					String[] msgRcvd = msg.split(":"); //JOIN:myPort

					nodeState = NodeState.valueOf(msgRcvd[0]);
					Log.d(TAG, "Message recieved : " + msg);

					switch (nodeState) {
						case NEW_NODE:		//in 11108
							String value_rec = msgRcvd[1].trim();
							if(value_rec.equals("11108")){
								if(exists()) {
									Log.d(TAG, "Node recovery case for 11108");
									//	set node_join_list with all 5 values
									node_join_list.add("11108");
									node_join_list.add("11112");
									node_join_list.add("11116");
									node_join_list.add("11120");
									node_join_list.add("11124");

									Log.d(TAG,"Node recovery status sent to : " + value_rec);
									try {
										//String port = "11108";
										Socket socket_local = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(value_rec.trim()));
										DataOutputStream out_local = new DataOutputStream(socket_local.getOutputStream());

										Thread.sleep(10);
										out_local.writeUTF(NodeState.RECOVERY + ":" + value_rec);
										out_local.flush();
										Log.d(TAG, NodeState.RECOVERY + ":" + value_rec + " sent to " + value_rec);

										//Thread.sleep(20);
										out_local.close();
										socket.close();
									} catch (InterruptedException e) {
										e.printStackTrace();
									} catch (UnknownHostException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}

								}
								else{
								 //new node case
									Log.d(TAG, "New node case for 11108");
									node_join_list.add("11108");
								}
							}

							else{
								if(node_join_list.contains(value_rec)) {
									//write recovery on same node
									Log.d(TAG,"Node recovery status sent to : " + value_rec);
									try {
										//String port = "11108";
										Socket socket_local = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(value_rec.trim()));
										DataOutputStream out_local = new DataOutputStream(socket_local.getOutputStream());

										Thread.sleep(10);
										out_local.writeUTF(NodeState.RECOVERY + ":" + value_rec);
										out_local.flush();
										Log.d(TAG, NodeState.RECOVERY + ":" + value_rec + " sent to " + value_rec);

										//Thread.sleep(20);
										out_local.close();
										socket.close();
									} catch (InterruptedException e) {
										e.printStackTrace();
									} catch (UnknownHostException e) {
										e.printStackTrace();
									} catch (IOException e) {
										e.printStackTrace();
									}
								}else{
									Log.d(TAG, "New node case for node " + value_rec);
									node_join_list.add(value_rec);
									Log.d(TAG, Arrays.asList(node_join_list.toArray()).toString());
								}

							}
							break;

						case INSERT:

							Log.d(TAG, "Executing case " + nodeState);

							String[] input = msgRcvd[1].split("-");

							ContentValues keyValue = new ContentValues();
							keyValue.put(SimpleDynamoDbHelper.KEY_FIELD, input[0].trim());
							keyValue.put(SimpleDynamoDbHelper.VALUE_FIELD, input[1].trim());
							keyValue.put(SimpleDynamoDbHelper.PORT_FIELD, input[2].trim());

							final SQLiteDatabase db3 = simpleDynamoDbHelper.getWritableDatabase();

							db3.replace(TABLE_NAME, null, keyValue);

							Log.d(TAG, "Inserting key-value " + input[0].trim() + "-" + input[1].trim() + " inserted in port " + Integer.parseInt(keyValue.getAsString(SimpleDynamoDbHelper.PORT_FIELD)) / 2);
							break;

						case QUERYALL:
							initialNode = msgRcvd[1].trim();
							String keyValuePair = "";

							Log.d(TAG, "Executing " + nodeState + " initial Node value " + initialNode);

							Cursor cursor = query(BASE_URI, null, "@", null, null);
							while (cursor.moveToNext()) {
								key = cursor.getString(cursor.getColumnIndex(SimpleDynamoDbHelper.KEY_FIELD));
								value = cursor.getString(cursor.getColumnIndex(SimpleDynamoDbHelper.VALUE_FIELD));

								keyValuePair += key + "-" + value + ";";
							}

							Log.d(TAG, "Keyvalue - pair -(1) " + keyValuePair);
							out = new DataOutputStream(socket.getOutputStream());

							Thread.sleep(10);
							out.writeUTF(keyValuePair);
							out.flush();

							out.close();
							socket.close();
							break;

						case QUERYKEY:
							String search_key = msgRcvd[1].trim();

							Log.d(TAG, "Executing " + nodeState + " querying key " + search_key);

							final SQLiteDatabase db1 = simpleDynamoDbHelper.getWritableDatabase();
							Cursor cursor1 = db1.query(TABLE_NAME, null, "key='" + search_key + "'", null, null, null, null);

							while (cursor1.moveToNext()) {
								key = cursor1.getString(cursor1.getColumnIndex(SimpleDynamoDbHelper.KEY_FIELD));
								value = cursor1.getString(cursor1.getColumnIndex(SimpleDynamoDbHelper.VALUE_FIELD));
							}

							Log.d(TAG, "Key-value - pair -(2) " + key + " " + value);
							out = new DataOutputStream(socket.getOutputStream());
							Thread.sleep(100);
							out.writeUTF(key + "-" + value);
							out.flush();

							out.close();
							socket.close();
							break;

						case DELETEALL:

							final SQLiteDatabase db6 = simpleDynamoDbHelper.getWritableDatabase();

							db6.delete(TABLE_NAME, null, null);
							break;

						case DELETEKEY:
							String dkey = msgRcvd[1].trim();

							final SQLiteDatabase db5 = simpleDynamoDbHelper.getWritableDatabase();

							db5.delete(TABLE_NAME, "key='" + dkey + "'", null);
							break;

						case TEST:
							DataOutputStream d_out = new DataOutputStream(socket.getOutputStream());
							Thread.sleep(10);
							d_out.writeUTF("ACK");
							d_out.flush();
							d_out.close();
							break;

						case RECOVERY:
							String port = myPort.trim();

							Log.d(TAG, "Executing case: " + NodeState.RECOVERY);

							String predecessor_predecessor = null;
							String predecessor = null;
							String successor = null;
							String successor_successor = null;

							for (int i = 0; i < sortedMembershipList.length; i++) {
								if(port.equals(sortedMembershipList[i])) {
									if (i == 0) {
										predecessor_predecessor = sortedMembershipList[3];
										predecessor = sortedMembershipList[4];
										successor = sortedMembershipList[1];
										successor_successor = sortedMembershipList[2];
									} else if(i == 1){
										predecessor_predecessor = sortedMembershipList[4];
										predecessor = sortedMembershipList[0];
										successor = sortedMembershipList[2];
										successor_successor = sortedMembershipList[3];
									} else if(i == 2){
										predecessor_predecessor = sortedMembershipList[0];
										predecessor = sortedMembershipList[1];
										successor = sortedMembershipList[3];
										successor_successor = sortedMembershipList[4];
									}	else if (i == 3) {
										predecessor_predecessor = sortedMembershipList[1];
										predecessor = sortedMembershipList[2];
										successor = sortedMembershipList[4];
										successor_successor = sortedMembershipList[0];
									} else if (i == 4) {
										predecessor_predecessor = sortedMembershipList[2];
										predecessor = sortedMembershipList[3];
										successor = sortedMembershipList[0];
										successor_successor = sortedMembershipList[1];
									}

									predecessor_predecessor = predecessor_predecessor.trim();
									predecessor = predecessor.trim();
									successor = successor.trim();
									successor_successor = successor_successor.trim();

									break;
								}
							}

							delete(BASE_URI, "@", null);

							//Quorum Node Replication Factor is 2, similar to Dynamo Db
							ArrayList<String> succ_list = new ArrayList<String>();
							succ_list.add(successor);
							succ_list.add(successor_successor);

							//Recovering the local data from successors
							recoverLocalData(succ_list);

							ArrayList<String> pred_list = new ArrayList<String>();
							pred_list.add(predecessor);
							pred_list.add(predecessor_predecessor);

							//Recovering the replica copies from predecessors
							recoverReplicaData(pred_list);

							break;

						case RECOVERDATA:

							Log.d(TAG, "Executing case: " + NodeState.RECOVERDATA);

							String r_port = msgRcvd[1].trim();
							String kv_port_pair = "";

							Cursor cursor3 = query(BASE_URI,null, r_port, null, null);

							if(null != cursor3) {
								while (cursor3.moveToNext()) {
									key = cursor3.getString(cursor3.getColumnIndex(SimpleDynamoDbHelper.KEY_FIELD));
									value = cursor3.getString(cursor3.getColumnIndex(SimpleDynamoDbHelper.VALUE_FIELD));

									kv_port_pair += key + "-" + value + "-" + r_port + ";";
								}

								out = new DataOutputStream(socket.getOutputStream());

								Log.d(TAG,"Testing the null condition: " + kv_port_pair);
								Thread.sleep(20);

								out.writeUTF(kv_port_pair);
								out.flush();

								out.close();
								socket.close();
							}

							break;

					}
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	//Checking if the Node is available
	public boolean exists(){
		try {
			Socket new_socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), 11112);
			DataOutputStream succ_out = new DataOutputStream(new_socket.getOutputStream());

			Thread.sleep(10);
			succ_out.writeUTF(NodeState.TEST + ":test");
			succ_out.flush();
			Log.d(TAG, NodeState.TEST + ":test");

			DataInputStream succ_in = new DataInputStream(new_socket.getInputStream());
			String inp = succ_in.readUTF().trim();
			if(inp!=null){
				if(inp.equals("ACK")){
					succ_out.close();
					new_socket.close();

					return true;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return false;
	}

	private class ClientTask extends AsyncTask<String, Void, Void> {

		@Override
		protected Void doInBackground(String... msg) {
			try {
				String port = "11108";
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port.trim()));
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());

				Thread.sleep(10);
				out.writeUTF(NodeState.NEW_NODE + ":" + msg[0].trim());
				out.flush();
				Log.d(TAG, NodeState.NEW_NODE + ":" + msg[0].trim() + " sent to 11108");

				//Thread.sleep(20);
				out.close();
				socket.close();
			} catch (InterruptedException e) {
				e.printStackTrace();
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}
	}

	@Override
	public Uri insert(Uri uri, ContentValues values) {
		// TODO Auto-generated method stub

		String key = values.getAsString(SimpleDynamoDbHelper.KEY_FIELD);
		String value = values.getAsString(SimpleDynamoDbHelper.VALUE_FIELD);

		ArrayList<String> pList = getBucket(key);
		Log.d(TAG, "insert() Bucket String: " + Arrays.toString(pList.toArray()));

		//Quorum Write factor is 2
		try {
			for (String port : pList) {
				Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());

				Thread.sleep(100);
				out.writeUTF(NodeState.INSERT + ":" + key + "-" + value + "-" + pList.get(0));
				out.flush();

				Log.d(TAG, "insert() Message to be inserted in " + NodeState.INSERT + ":" + key + "-" + value );

//				Thread.sleep(20);
				out.close();
				socket.close();
			}
		} catch (UnknownHostException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return null;
	}


	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
						String[] selectionArgs, String sortOrder) {
		// TODO Auto-generated method stub
		Cursor cursor = null;
		final SQLiteDatabase db = simpleDynamoDbHelper.getWritableDatabase();

		if(selection.equals("@")){
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			cursor = db.query(SimpleDynamoDbHelper.TABLE_NAME, new String[]{SimpleDynamoDbHelper.KEY_FIELD, SimpleDynamoDbHelper.VALUE_FIELD}, null, null, null, null, null);
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}else if(selection.equals("*")) {

			String msgRcvd = null;
			String keyValuePair = "";
			String result = "";

			for(String port: sortedMembershipList) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port.trim()));
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());

					Thread.sleep(10);
					out.writeUTF(NodeState.QUERYALL + ":" + "query");
					out.flush();
					Log.d(TAG, "Message sent to the successor " + Integer.parseInt(port.trim()) + " > " + NodeState.QUERYALL + ":" + "query" + "(1)");

					DataInputStream in = new DataInputStream(socket.getInputStream());
					msgRcvd = in.readUTF();

					Log.d(TAG, "Message received from the query forward: " + msgRcvd + " from " + port);
					keyValuePair += msgRcvd;

//					Thread.sleep(20);
					out.close();
					in.close();
					socket.close();

//					Log.d(TAG, "Message received from the query forward: " + msgRcvd + " from " + successor);

				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}


			if (msgRcvd == null || msgRcvd.isEmpty()) {
				result = keyValuePair;
			}else if(keyValuePair == null || keyValuePair.isEmpty()){
				result = msgRcvd;
			}else if((msgRcvd == null || msgRcvd.isEmpty()) && (keyValuePair == null || keyValuePair.isEmpty())){
				result = "";
			}else {
				result = keyValuePair + ";" + msgRcvd;
			}


			Log.d(TAG, "Final Query * Result: " + result);
			String[] keyValue = result.trim().split(";");

			Log.d(TAG, "Key Value pair " + keyValue.toString());

			MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDynamoDbHelper.KEY_FIELD, SimpleDynamoDbHelper.VALUE_FIELD});
			if(!result.isEmpty()) {
				for (String kv : keyValue) {
					if(!kv.isEmpty()) {
						String[] kPair = kv.trim().split("-");
						Log.d(TAG, "KV pair " + kv);
						matrixCursor.addRow(new String[]{kPair[0].trim(), kPair[1].trim()});
					}
				}
			}

			cursor = matrixCursor;

			Log.d(TAG, "Final Query Result RowCount: " + cursor.getCount());
		}else if(null != selection && selection.length() > 5) {
			String key = selection;

			Log.d(TAG, "Searching for the key: " + key + " in myPort " + myPort);

			ArrayList<String> pList = getBucket(key);
			String msgRcvd = null;

			//Quorum Read factor is 2
			for (String port : pList) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port));
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());

					Thread.sleep(10);
					out.writeUTF(NodeState.QUERYKEY + ":" + key);
					out.flush();

					DataInputStream in = new DataInputStream(socket.getInputStream());
					msgRcvd = in.readUTF();

					Thread.sleep(100);
					out.close();
					in.close();
					socket.close();

					Log.d(TAG, "Querying key: " + selection + " message returned from "+ port + " with message: " + msgRcvd);
					break;
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

			MatrixCursor matrixCursor = new MatrixCursor(new String[]{SimpleDynamoDbHelper.KEY_FIELD, SimpleDynamoDbHelper.VALUE_FIELD});
			if(msgRcvd!=null && !(msgRcvd.length() ==1)) {
				String[] kPair = msgRcvd.trim().split("-");
				matrixCursor.addRow(new String[]{kPair[0].trim(), kPair[1].trim()});

				cursor = matrixCursor;
			}

		}else {
			String port = selection;
			final SQLiteDatabase db4 = simpleDynamoDbHelper.getWritableDatabase();

			cursor = db4.query(TABLE_NAME, null, "port='" + port + "'", null, null, null, null);
			Log.d(TAG, "Querying based on port count: " + cursor.getCount() + " for " + port);

		}
		return cursor;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		// TODO Auto-generated method stub
		final SQLiteDatabase db = simpleDynamoDbHelper.getWritableDatabase();

		int deletedRows = 0;

		if (selection.equals("@")) {
			Log.d(TAG, "Content Provider - delete() @ condition started");
			deletedRows = db.delete(TABLE_NAME, null, null);
		} else if (selection.contains("*")) {
			for(String port: sortedMembershipList) {
				try {
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port.trim()));
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());

					Thread.sleep(10);
					out.writeUTF(NodeState.DELETEALL + ":" + "delete");
					out.flush();
					Log.d(TAG, "Message sent to the successor " + Integer.parseInt(port.trim()) + " > " + NodeState.DELETEALL + ":" + "delete");

					out.close();
					socket.close();

//					Log.d(TAG, "Message received from the query forward: " + msgRcvd + " from " + successor);

				}  catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}else {
			String key = selection.trim();
			ArrayList<String> pList = getBucket(key);

			for (String port : pList) {
				try {
					Log.d(TAG, "Deleting the key " + key + " in " + Integer.parseInt(port)/2);
					Socket socket = new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(port.trim()));
					DataOutputStream out = new DataOutputStream(socket.getOutputStream());


					Log.d("DELETE KEY",NodeState.DELETEKEY + ":" + key );

					Thread.sleep(10);
					out.writeUTF(NodeState.DELETEKEY + ":" + key + " ");
					out.flush();

//					Thread.sleep(10);
					out.close();
					socket.close();
				} catch (UnknownHostException e) {
					e.printStackTrace();
					Log.d(TAG, "IOException " + port);
				} catch (Exception e) {
					e.printStackTrace();
					Log.d(TAG, "IOException " + port);
				}
			}

		}
		return deletedRows;
	}

	@Override
	public String getType(Uri uri) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int update(Uri uri, ContentValues values, String selection,
					  String[] selectionArgs) {
		// TODO Auto-generated method stub
		return 0;
	}

	//Bucketing of data with consistent hashing
	private ArrayList<String> getBucket(String key){
		String predecessor = null;
		String successor = null;
		String successor_successor = null;

		ArrayList<String> portList = new ArrayList<String>();

		for (int i = 0; i < sortedMembershipList.length; i++) {
			String port = sortedMembershipList[i];
			if (i == 0) {
				predecessor = sortedMembershipList[sortedMembershipList.length - 1];
				successor = sortedMembershipList[i + 1];
				successor_successor = sortedMembershipList[i + 2];
			} else if (i == sortedMembershipList.length - 1) {
				predecessor = sortedMembershipList[sortedMembershipList.length - 2];
				successor = sortedMembershipList[0];
				successor_successor = sortedMembershipList[1];
			} else if (i == 3) {
				predecessor = sortedMembershipList[i - 1];
				successor = sortedMembershipList[i + 1];
				successor_successor = sortedMembershipList[0];
			} else {
				predecessor = sortedMembershipList[i - 1];
				successor = sortedMembershipList[i + 1];
				successor_successor = sortedMembershipList[i + 2];
			}

			predecessor = predecessor.trim();
			port = port.trim();
			successor = successor.trim();
			successor_successor = successor_successor.trim();

			try {
				if ((predecessor == null && successor == null) || (genHash(key).compareTo(genHash(String.valueOf(Integer.parseInt(predecessor) / 2))) > 0 &&
						genHash(key).compareTo(genHash(String.valueOf(Integer.parseInt(port) / 2))) <= 0) ||
						(genHash(String.valueOf(Integer.parseInt(predecessor) / 2)).compareTo(genHash(String.valueOf(Integer.parseInt(port) / 2))) > 0 &&
								(genHash(key).compareTo(genHash(String.valueOf(Integer.parseInt(predecessor) / 2))) > 0 ||
										genHash(key).compareTo(genHash(String.valueOf(Integer.parseInt(port) / 2))) <= 0))) {

					portList.add(port);
					portList.add(successor);
					portList.add(successor_successor);
				}
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
		}
		return portList;
	}

	private void recoverLocalData(ArrayList<String> nList){
		String msgRcvd = null;
		String keyValuePair = "";

		Log.d(TAG,"Executing recoverLocalData()");
		for (String node: nList){
			try{
			Socket socket =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.trim()));
			DataOutputStream out = new DataOutputStream(socket.getOutputStream());

			Thread.sleep(10);
			out.writeUTF(NodeState.RECOVERDATA + ":" + myPort + " ");
			out.flush();

			DataInputStream in = new DataInputStream(socket.getInputStream());
			msgRcvd = in.readUTF().trim();

			Log.d(TAG, "Executing recoverLocalData() local replica Message Received : " + msgRcvd + " from " + node);
			Thread.sleep(10);
			keyValuePair += msgRcvd;

			out.close();
			in.close();
			socket.close();

			//break;
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if(msgRcvd != null && !(msgRcvd.length() == 0)) {

			Log.d(TAG,"Executing recoverLocalData() appended keyValue pair" + keyValuePair);

			String[] keyValue = msgRcvd.trim().split(";");

			for (String kv : keyValue) {
				String[] input = kv.split("-");

				ContentValues kValue = new ContentValues();
				kValue.put(SimpleDynamoDbHelper.KEY_FIELD, input[0].trim());
				kValue.put(SimpleDynamoDbHelper.VALUE_FIELD, input[1].trim());
				kValue.put(SimpleDynamoDbHelper.PORT_FIELD, input[2].trim());

				final SQLiteDatabase db3 = simpleDynamoDbHelper.getWritableDatabase();

				db3.replace(TABLE_NAME, null, kValue);
			}

			Log.d(TAG, "Executing recoverLocalData() total count " + keyValue.length);
		}

	}

	private void recoverReplicaData(ArrayList<String> nList){
		String msgRcvd = null;
		String keyValuePair = "";

		Log.d(TAG,"Executing recoverReplicaData()");
		for (String node: nList){
			try{
				Socket socket =  new Socket(InetAddress.getByAddress(new byte[]{10, 0, 2, 2}), Integer.parseInt(node.trim()));
				DataOutputStream out = new DataOutputStream(socket.getOutputStream());

				Thread.sleep(10);
				out.writeUTF(NodeState.RECOVERDATA + ":" + node + " ");
				out.flush();

				DataInputStream in = new DataInputStream(socket.getInputStream());
				msgRcvd = in.readUTF().trim();

				Log.d(TAG, "Executing recoverReplicaData() "+ node +" replica Message Received : " + msgRcvd + " from " + node);

				keyValuePair += msgRcvd;

				Thread.sleep(10);

				out.close();
				in.close();
				socket.close();

				//break;
			} catch (UnknownHostException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if(msgRcvd != null && !(msgRcvd.length() == 0)) {

			Log.d(TAG,"Executing recoverReplicaData() returning keyValue pair" + keyValuePair);

			String[] keyValue = keyValuePair.trim().split(";");

			for (String kv : keyValue) {
				String[] input = kv.split("-");

				ContentValues kValue = new ContentValues();
				kValue.put(SimpleDynamoDbHelper.KEY_FIELD, input[0].trim());
				kValue.put(SimpleDynamoDbHelper.VALUE_FIELD, input[1].trim());
				kValue.put(SimpleDynamoDbHelper.PORT_FIELD, input[2].trim());

				final SQLiteDatabase db3 = simpleDynamoDbHelper.getWritableDatabase();

				db3.replace(TABLE_NAME, null, kValue);
			}

			Log.d(TAG, "Executing recoverReplicaData() total count " + keyValue.length);
		}

	}

	private String genHash(String input) throws NoSuchAlgorithmException {
		MessageDigest sha1 = MessageDigest.getInstance("SHA-1");
		byte[] sha1Hash = sha1.digest(input.getBytes());
		Formatter formatter = new Formatter();
		for (byte b : sha1Hash) {
			formatter.format("%02x", b);
		}
		return formatter.toString();
	}
}
