
import java.net.*;
import java.io.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.*;
import java.util.HashMap;
import java.util.List;
/**
 * @author Shuhao Zhou
 *
 */
public class owner {
	private static final int sPort = 8050; // The owner will be listening on this port number
	static ArrayList<ArrayList<Integer>> peer_chunk_list = new ArrayList<ArrayList<Integer>>(); //嵌套动态数组，外层代表不同的Peer，内层是这个peer的chunk list
	static int chunk_counter = 0;
	/**
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		// RUNNING

		// create listener to the socket
		ServerSocket listener = new ServerSocket(sPort); //owner创建的socket用来监听client的请求
		System.out.println("The fileowner is running."); //监听端口已经开启
		int peerNum = 0;
		int peerTotal = 5;

		//chop into chunks
		int chunk_size = 102400; //每个chunk 100KB = 102400 byte
		byte[] buffer = new byte[chunk_size]; //buffer是用来读文件的，要存多个byte，要用数组。buffer数字大小100，可以小于
		BufferedInputStream cbis = new BufferedInputStream (new FileInputStream("test.pdf"));//FileInputStream 是读文件的流，A FileInputStream obtains input bytes from a file in a file system. FileInputStream is meant for reading streams of raw bytes.  BufferedInputStream可以生成一个内部buffer数组，方便切。
		int bytesRead; //读了多少个byte，没有内容只是数字
		// 此处问题好大，第二个和第三个参数省略代表默认值
		while ((bytesRead = cbis.read(buffer)) >0) { //一直往后读，把数据读进buffer，大小为bytesRead
			File chunkFile = new File(Integer.toString(chunk_counter)); // 给一个路径，该路径下文件不存在，就创建文件，文件存在，就读取该文件。此处创建文件，Creates a new File instance by converting the given pathname string into an abstract pathname.
			System.out.println("Chunk " + Integer.toString(chunk_counter) + " is created");
			chunk_counter++; //一共40
			//输出流用来写入
			FileOutputStream cout = new FileOutputStream(chunkFile); //创建存取文件的 FileOutputStream 对象, Creates a file output stream to write to the file represented by the specified File object.
			cout.write(buffer, 0, bytesRead); //Writes (bytesRead) bytes from the specified byte array (buffer) starting at offset (0) to this file output stream.
			cout.close(); //关闭 FileOutputStream
		}

		//split chunk to 5 peers

		ArrayList<Integer> chunk_left = new ArrayList<Integer>(); //动态数组，没发出的chunk
		//for example: chunk_left = [0,1,2,3,4,5,6]
		for (int i = 0; i < chunk_counter; i++) { //把chunks的编号0～39全都存到chunk_left数组里
			chunk_left.add(i);
		}
		for (int i = 0; i < 5; i++) {
			ArrayList<Integer> peer_chunk = new ArrayList<Integer>(); //某一个peer拥有的的chunk的动态数组
			if (i != 4) { //如果不是最后一个peer
				for (int j = 0; j < chunk_counter/5; j++) { // 前四个peer每个最多接受40/5 = 8个随机的chunk
					//Random(): Creates a new random number generator.
					//nextInt: Returns a pseudorandom, uniformly distributed int value between 0 (inclusive) and the specified value (exclusive), drawn from this random number generator's sequence
					int index = new Random().nextInt(chunk_left.size()); //生成随机index
					peer_chunk.add(chunk_left.get(index)); //peer_chunk 动态数组里加入了随机取出的 chunk 的编号
					chunk_left.remove(index);
				}
			} else { // 最后一个peer
				for (int j = 0; j < chunk_left.size(); j++) {
					peer_chunk.add(chunk_left.get(j)); //动态数组的index会随着成员的增加和减少而变化，所以如果剩下8个element，那 j == 0 ～ 7
				}
			}
			peer_chunk_list.add(peer_chunk); //把peer_chunk　这个list 放进总的嵌套的list里。peer_chunk_list有5个成员，每个成员都是一个peer的动态表
		}


		try {
			while (true) {
				new Handler(listener.accept(), peerNum).start(); // 每accept一个peer的socket，就新开一个thread，5个thread同时处理5个peer
				System.out.println("Peer " + peerNum + " is connected!");
				peerNum++;
			}
		} finally {
			listener.close();
		}
	}

	/**
	 * A handler thread class. Handlers are spawned from the listening loop and are
	 * responsible for dealing with a single client's requests.
	 */
	private static class Handler extends Thread { //Handler类 继承了Thread类 ，handler 是一个线程
		private String message; // message received from the client
		private String MESSAGE; // uppercase message send to the client
		private Socket connection;
		private ObjectInputStream in; // stream read from the socket
		private ObjectOutputStream out; // stream write to the socket
		private int no; // The index number of the client

		public Handler(Socket connection, int no) { //constructor
			this.connection = connection; //该 peer 跟 owner 连接的 socket
			this.no = no; //第几个peer
		}

		public void run() {
			try {

				// initialize InputStream and OutputStream
				out = new ObjectOutputStream(connection.getOutputStream()); // get an output stream for writing bytes to this socket.
				// 大问题
				out.flush();// 刷新缓存 write any buffered output bytes and flush through to the underlying stream
				in = new ObjectInputStream(connection.getInputStream());

				// start sending file

				MESSAGE = Integer.toString(chunk_counter);
				sendMessage(MESSAGE); //sent to client

				MESSAGE = "START";
				sendMessage(MESSAGE);

				for (int i = 0; i < peer_chunk_list.get(no).size(); i++) { //每个peer的list的chunk数（接受多少个）
				//for (int i = 0; i < 7; i++) {
					File file_upload = new File(Integer.toString(peer_chunk_list.get(no).get(i)));//给一个路径，该路径下文件不存在，就创建文件，文件存在，就读取该文件。chunk的编号为文件名，读取目录中的chunk文件
					MESSAGE = Integer.toString(peer_chunk_list.get(no).get(i)); //获取chunk名
					//File file_upload = new File(Integer.toString(i));
					//MESSAGE = Integer.toString(i);
					sendMessage(MESSAGE); // 发出chunk名
					MESSAGE = Integer.toString((int) file_upload.length()); //获取chunk的大小
					sendMessage(MESSAGE);//CHUNK的大小发过去
					byte[] file_byte = new byte[(int) file_upload.length()];//搞一个跟文件大小一样的byte数组，做buffer
					BufferedInputStream bis = new BufferedInputStream(new FileInputStream(file_upload)); //把文件读成流
					bis.read(file_byte, 0, file_byte.length); //读取
					sendFile(file_byte); //文件作为流发出
					bis.close();
					message = (String) in.readObject(); //读peer发来的信息
					System.out.println(message + " by peer " + no);
				}
				//in.close();
				//connection.close();
				System.out.println("All chunks are sent"); //该peer的chunk传完 "of peerNum"
				//end sending file
				MESSAGE = "END";
				sendMessage(MESSAGE);
				//out.close();

				message = (String) in.readObject();
				System.out.println(message + " by peer " + no);
				out.close();
				in.close();
				connection.close(); //关闭socket（连接）
			} catch (IOException ioException) {
				System.out.println("Disconnect with peer " + no);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// send a message to the output stream
		public void sendMessage(String msg) {
			try {
				out.writeObject(msg); //Write the specified object to the ObjectOutputStream
				out.flush();
				System.out.println("Send Chunk: " + msg + " to peer " + no);
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}

		void sendFile(byte[] file_byte) {
			try {
				out.write(file_byte, 0, file_byte.length); //Writes a sub array of bytes. （file_byte) the data to be written, (0) the start offset in the data,  (file_byte.lengt) the number of bytes that are written
				out.flush();
			} catch (IOException ioException) {
				ioException.printStackTrace();
			}
		}
	}
}
