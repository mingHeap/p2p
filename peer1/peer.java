
import java.net.*;
import java.io.*;
import java.util.*;
import java.lang.String;

import java.util.concurrent.locks.ReentrantLock;
/**
 * @author Shuhao Zhou
 * UFID 37137157
 * This is project2
 */
public class peer {
	int fo_lp; // file owner listening port
	int p_lp; // peer listening port
	int dn_lp; // download neighbor listening port
	Socket requestSocket; // socket connect to the file owner
	ObjectOutputStream out; // stream write to the socket
	ObjectInputStream in; // stream read from the socket
	String message; // message send to the server
	String MESSAGE; // capitalized message read from the server

	//private static final int pPort = 8063;
	static int totalChunk = 0;
	// 每个peer都自己维护一张chunk_list表，每次收到chunk会更新，表同时被读写会出错。用lock可以实现"写的时候不能读，读的时候不能写"；
	//每个peer有两个线程，一个收一个发，两个线程同时用一张表，一个读一个写会冲突，同时写也会冲突，同时读不会。
	static ReentrantLock lock = new ReentrantLock();
	static ReentrantLock lock2 = new ReentrantLock();

	public void peer() {
	}

	void running() {
		try {
			// Ask for three port number
			// File owner's listening port, peer's listening port
			// and download neighbor's listening port
			BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(System.in)); //丛命令行读取
			// format "xxxx xxxx xxxx"
			System.out.println("Please enter three listening ports to start your peer process: ");
			String ports = bufferedReader.readLine();
			String[] temp = ports.split(" ");

			// Checking the input
			while (temp.length != 3 || !allInteger(temp)) {
				System.out.println("Please enter three Integer!: ");
				ports = bufferedReader.readLine();
				temp = ports.split(" ");
			}

			// get each listening port
			fo_lp = Integer.parseInt(temp[0]);
			p_lp = Integer.parseInt(temp[1]);
			dn_lp = Integer.parseInt(temp[2]);

			// Print out each port on peer side
			System.out.println("File owner's listening port is: " + Integer.toString(fo_lp));
			System.out.println("Peer's listening port is: " + Integer.toString(p_lp));
			System.out.println("Download neighbor's listening port is: " + Integer.toString(dn_lp));

			// connect to file owner
			requestSocket = new Socket("localhost", fo_lp);
			System.out.println("Connected to file owner in port " + Integer.toString(fo_lp));

			// initialize inputStream and outputStream
			out = new ObjectOutputStream(requestSocket.getOutputStream());
			out.flush();
			in = new ObjectInputStream(requestSocket.getInputStream());

			// start getting chunks
			MESSAGE = (String)in.readObject();
			totalChunk = Integer.parseInt(MESSAGE); // 40个

			ArrayList<Integer> ownedArray1 = new ArrayList<Integer>();
			MESSAGE = (String) in.readObject();
			if (MESSAGE.equals("START")) {
				while (!(MESSAGE = (String)in.readObject()).equals("END")) {
					String file_name = MESSAGE;
					ownedArray1 = getOwnedArray();//从文件中获取表格
					ownedArray1.add(Integer.parseInt(file_name)); //给表哥添加已有chunk id
					storeArray(ownedArray1); //更新文件
					File file_download = new File(file_name);
					file_download.createNewFile(); //createNewFile(): boolean; true if the named file does not exist and was successfully created; false if the named file already exists
					MESSAGE = (String)in.readObject();
					int total_size = Integer.parseInt(MESSAGE);
					//byte[] file_byte = new byte[1024];
					BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file_download)); // 往文件里写
					int bytesRead;
					while (total_size >0) {
						byte[] file_byte = new byte[total_size];
						bytesRead = in.read(file_byte, 0, file_byte.length);
						bos.write(file_byte, 0, bytesRead);
						total_size = total_size - bytesRead;
					}
					bos.close();
					System.out.println("Chunk " + file_name + " is received");
					message = "Chunk " + file_name + " is received";
					sendMessage(message);
				}

				//storeArray(ownedArray);

				message = "All chunks from owner are received.";
				sendMessage(message);
			}

			int peerNum = 0;

			//start one server two Sockets
			ServerSocket listener = new ServerSocket(p_lp);
			System.out.println("peer server is running");
			//Socket socketp_lp = new Socket("localhost", p_lp);
			//System.out.println("socketp server is running");
			//Socket socketdn_lp = new Socket("localhost", dn_lp);
			try {
				new Handler2(dn_lp).start();
				System.out.println("peer client is running");
				//new Handler3(dn_lp).start();
				while (true) {
					new Handler(listener.accept(), peerNum).start();
					System.out.println("Peer is connected to peer server!");
					peerNum++;
				}
			} finally {
				listener.close();
			}
		} catch (ConnectException e) {
			System.err.println("Connection refused. You need to initiate a server first.");
		} catch (UnknownHostException unknownHost) {
			System.err.println("You are trying to connect to an unknown host!");
		} catch (IOException ioException) {
			ioException.printStackTrace();
		} catch ( ClassNotFoundException e ) {
            		System.err.println("Class not found");
        }
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		peer p = new peer();
		p.running();
	}

	boolean allInteger(String[] x) {
		for (String s : x) {
			if (!isInteger(s)) {
				return false;
			}
		}
		return true;
	}

	boolean isInteger(String s) {
		try {
			Integer.parseInt(s); //Parses the string argument as a signed decimal integer. The characters in the string must all be decimal digits, except that the first character may be an ASCII minus sign '-' '+'
		} catch (NumberFormatException ex) {
			return false;
		}

		return true;
	}

	// send a message to the output stream
	public void sendMessage(String msg) {
		try {
			// stream write the message
			out.writeObject(msg);
			out.flush();
		} catch (IOException ioException) {
			ioException.printStackTrace();
		}
	}


		private static class Handler2 extends Thread {
			private ArrayList<Integer> ownedArray2 = new ArrayList<Integer>();
			private Socket socketdn_lp;
			private ArrayList<Integer> array_message;
			private String string_message;
			private String MESSAGE1;
			private ObjectOutputStream out1;
			private ObjectInputStream in1;

			public Handler2(int socketdn) {
				boolean success  = false;
				while (!success) {
				try {
					this.socketdn_lp = new Socket("localhost", socketdn);
					success = true;
					System.out.println("Connected to peer server in port " + Integer.toString(socketdn));
				}
				catch (ConnectException e) {
					System.err.println("Connection refused. You need to initiate a server first.");
				} catch (UnknownHostException unknownHost) {
					System.err.println("You are trying to connect to an unknown host!");
				} catch (IOException ioException) {
					ioException.printStackTrace();
				}
			}
		}

			public void run() {
				try {
					//Thread.sleep(20000);
					out1 = new ObjectOutputStream(socketdn_lp.getOutputStream());
					out1.flush();
					in1 = new ObjectInputStream(socketdn_lp.getInputStream());
					while (getOwnedArray().size() != totalChunk) {
						ownedArray2 = getOwnedArray();
						sendObject(ownedArray2);
						//while (!(MESSAGE1 = (String)in1.readObject()).equals("END")) {
							if (!(MESSAGE1 = (String)in1.readObject()).equals("SAME")) {
							String file_name = MESSAGE1;
							System.out.println("Peer now has chunk: ");
							System.out.print(ownedArray2);
							File file_download = new File(file_name);
							file_download.createNewFile();
							MESSAGE1 = (String)in1.readObject();
							int total_size = Integer.parseInt(MESSAGE1);
							//byte[] file_byte = new byte[1024];
							BufferedOutputStream bos = new BufferedOutputStream(new FileOutputStream(file_download));
							int bytesRead;
							while (total_size >0) {
								byte[] file_byte = new byte[total_size];
								bytesRead = in1.read(file_byte, 0, file_byte.length);
								bos.write(file_byte, 0, bytesRead);
								total_size = total_size - bytesRead;
							}
							bos.close();
							ownedArray2.add(Integer.parseInt(file_name));
							//lock.lock();
							//try {
							storeArray(ownedArray2);
							System.out.println("Chunk " + file_name + " is received");
						}}
				string_message = "DONE";
				sendObject(string_message);
				System.out.println("ALL CHUNKS HAVE BEEN RECEIVED");


			} catch (IOException ioException) {
					ioException.printStackTrace();
				} catch ( ClassNotFoundException e ) {
		      System.err.println("Class not found");
		    } finally {
					try {
					File name = new File("test.pdf");
					name.createNewFile();
					FileOutputStream fos = new FileOutputStream(name);

					for (int i = 0;i < totalChunk; i++) {
						File file_upload = new File(Integer.toString(i));
						byte[] file_byte = new byte[(int)file_upload.length()];
						BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file_upload));
						bis.read(file_byte, 0, file_byte.length);
						fos.write(file_byte);
						bis.close();
					}
					out1.close();
					in1.close();
					System.out.println("BYE");
				} catch(FileNotFoundException f) {
					System.out.println("File not found");

				} catch(IOException i) {
						System.out.println("ERROR");
				}
				}
			}
			// send a message to the output stream
			public void sendMessageArray (ArrayList<Integer> msg) {
				lock2.lock();
				try {
					// stream write the message
					out1.writeObject(msg);
					out1.flush();
				} catch (IOException ioException) {
					ioException.printStackTrace();
				} finally {
					lock2.unlock();
				}
			}

			public void sendObject(Object msg) {
				if(msg instanceof ArrayList) {
					msg = (ArrayList<Integer>)msg;
				} else {
					msg = (String)msg;
				}
				try {
					// stream write the message
					out1.writeObject(msg);
					out1.flush();
				} catch (IOException ioException) {
					ioException.printStackTrace();
				}
			}

			public void sendMessageString (String msg) {
				lock2.lock();
				try {
					// stream write the message
					out1.writeObject(msg);
					out1.flush();
				} catch (IOException ioException) {
					ioException.printStackTrace();
				} finally {
					lock2.unlock();
				}


		}
	}


		private static class Handler extends Thread {
			private Object message2; // message received from the client
			private ArrayList<Integer> message2_array;
			private String MESSAGE2; // uppercase message send to the client
			private Socket connection2;
			private ObjectInputStream in2; // stream read from the socket
			private ObjectOutputStream out2; // stream write to the socket
			private int no2; // The index number of the client
			private ArrayList<Integer> ownedArray3 = new ArrayList<Integer>();
			public Handler(Socket connection, int no) {
				this.connection2 = connection;
				this.no2 = no;
			}

			public void run() {
				try {
					//Thread.sleep(20000);
					// initialize InputStream and OutputStream
					 out2 = new ObjectOutputStream(connection2.getOutputStream());
					out2.flush();
					 in2 = new ObjectInputStream(connection2.getInputStream());
					 //while(true) {
					 //while(true) {
						 //try {
					while (!((message2 = in2.readObject()) instanceof String)) {
						//System.out.println("----------------------------1");
						//System.out.println(message2);
					message2_array = (ArrayList<Integer>)message2;
					//lock.lock();
					//try {
							ownedArray3= getOwnedArray();
					//} finally {
						//lock.unlock();
					//}
					ArrayList<Integer> transfer = compareArray(ownedArray3, message2_array);
					if (!transfer.isEmpty()) {
						//for (int i = 0; i < transfer.size(); i++) {
						//for (int i = 0; i < 7; i++) {
							int random = new Random().nextInt(transfer.size());
							File file_upload = new File(Integer.toString(transfer.get(random)));
							MESSAGE2 = Integer.toString(transfer.get(random));
							System.out.println("Send Chunk " + MESSAGE2 + " to peer client");
							//File file_upload = new File(Integer.toString(i));
							//MESSAGE = Integer.toString(i);
							sendMessage2(MESSAGE2);
							MESSAGE2 = Integer.toString((int) file_upload.length());
							sendMessage2(MESSAGE2);
							byte[] file_byte = new byte[(int) file_upload.length()];
							//lock.lock();
							//try {
							BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file_upload));
							bis.read(file_byte, 0, file_byte.length);
							sendFile2(file_byte);
							bis.close();
					} else {
						sendMessage2("SAME");
					}

				}

					//connection2.close();
				} catch (IOException ioException) {
					System.out.println("Disconnect with peer");
				} catch (ClassNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} //catch (InterruptedException i) {
			}

			public ArrayList<Integer> compareArray(ArrayList<Integer> x, ArrayList<Integer> y) {
				ArrayList<Integer> temp = new ArrayList<Integer>();
				for (int i=0; i < x.size(); i++) {
					if (!y.contains(x.get(i))) {
						temp.add(x.get(i));
					}
				}
				return temp;
			}

			// send a message to the output stream
			public void sendMessage2(String msg) {
				try {
					out2.writeObject(msg);
					out2.flush();
					if (!msg.equals("SAME")) {
					System.out.println("Send message: " + msg + " to peer");
				}
				} catch (IOException ioException) {
					ioException.printStackTrace();
				}
			}

			void sendFile2(byte[] file_byte) {
				try {
					out2.write(file_byte, 0, file_byte.length);
					out2.flush();
				} catch (IOException ioException) {
					ioException.printStackTrace();
				}
			}

		}
		public static ArrayList<Integer> getOwnedArray() {
			ArrayList<Integer> arraylist = new ArrayList<Integer>(); //存自己有的chunk编号的表
			lock.lock();
			try { // 把数据从文件读出来存到表里
				BufferedReader brTest = new BufferedReader(new FileReader("array.txt"));
				String array = brTest.readLine();
				if (array != null) {
					String[] starray = array.split(" ");
					for (int i = 0; i < starray.length; i++) {
						arraylist.add(Integer.parseInt(starray[i]));
					}
				}
				brTest.close();
			} catch(FileNotFoundException e) {
				System.err.println("get array problem");
			} catch(IOException e) {
				System.err.println("get array problem");
			} finally {
				lock.unlock();
			}
			return arraylist;
		}

		public static void storeArray(ArrayList<Integer> x){ // 把表写入文件，相当于更新文件
			lock.lock();
			try {
			FileWriter fw = new FileWriter("array.txt");
			String temp = "";
			for (int i=0;i<x.size();i++) {
				temp = temp + Integer.toString(x.get(i));
				temp = temp + " ";
			}
			fw.write(temp);
			fw.close();
		} catch (IOException e) {
			System.err.println("store array problem");
		} finally {
			lock.unlock();
		}
		}

}
