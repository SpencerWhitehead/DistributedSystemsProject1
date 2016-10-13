/**
 * Created by spencerwhitehead on 10/10/16.
 */

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

//public class Node implements Runnable {
public class Node {
    private int portNum;	//Port Number on which the Node.java will be Listening to accept connection
    private int ID;	        //NodeID of the node
    private HashMap neighbors = new HashMap(); // Neighbor node IDs and IP addresses (nodeID, IP)
    private HashMap holders = new HashMap(); // Holders of each file (filename, holderID)
    private HashMap asked = new HashMap(); // Map to store which files have been requested (filename, asked)
    private HashMap state = new HashMap(); // Map to store which files are being used by this node (filename, inUse)
    private HashMap reqQ = new HashMap(); // Map to store request queue for each file

    public Node(int port, int ident, HashMap<Integer,String> neigh) {
        this.portNum = port;
        this.ID = ident;
        this.neighbors = neigh;
    }

    public String[] parseCommand(String com){
        return com.split("\\s",3);
    }

    public void runCommand(String command, String fname, String contents){
        switch (command){
            case "create":
                System.out.println("Creating file");
//                createFile(String fname);
                break;
            case "delete":
                System.out.println("Deleting file");
//                deleteFile(String fname);
                break;
            case "read":
                System.out.println("Reading file");
//                readFile(String fname);
                break;
            case "append":
                if(contents != null) {
                    System.out.println("Appending "+contents+" to "+fname);
                }
//                appendFile(String fname, contents);
                break;
            default:
                System.out.println("Invalid command: "+command);
                break;
        }
    }

    public void takeCommand(String command){
        String[] com = parseCommand(command);
        if(com.length == 2){
            runCommand(com[0], com[1], null);
        }
        else if(com.length == 3){
            runCommand(com[0], com[1], com[2]);
        }
    }

    public void begin() {

        final ExecutorService clientProcessingPool = Executors.newFixedThreadPool(10);

        Runnable serverTask = new Runnable() {
            @Override
            public void run() {

                try {
                    ServerSocket serverSocket = new ServerSocket(portNum);

                    while (true) {
                        Socket clientSocket = serverSocket.accept();
                        clientProcessingPool.submit(new ConnectHandler(clientSocket));
                    }
                } catch (IOException e) {
                    System.err.println("Accept failed.");
                }
            }
        };
        Thread serverThread = new Thread(serverTask);
        serverThread.start();
    }

//    public void run(){
//        try(ServerSocket serve = new ServerSocket(portNum)){
//            int i = 0;
//            while(true){
//                if (i < 3) {
//                    System.out.println("WAITING FOR CONNECTION");
//                }
//                else{
//                    break;
//                }
//                Socket sock = serve.accept();
//                ConnectHandler c = new ConnectHandler(sock);
//                System.out.println("GOT A CONNECTION!");
//                c.run();
//                i++;
//            }
//        }
//        catch (IOException e) {
//            System.err.println("Could not listen on port " + portNum);
//            System.exit(-1);
//        }
//    }

    public class ConnectHandler implements Runnable {
        // Handle socket connection and receive and send messages
        private Socket socket = null;
        private BufferedReader is = null;
        private PrintStream os = null;

        public ConnectHandler(Socket socket) {this.socket = socket;}

        public void run(){
            try {
                System.out.println("HANDLING CONNECTION!");

                is = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                os = new PrintStream(socket.getOutputStream());

                while(true){
                    String s = is.readLine();
                    System.out.println(s);
                    os.println("GOT IT!");
                    System.out.println("SENT MESSAGE!");
                    break;
                }
                is.close();
                os.close();
                socket.close();
            }
            catch (IOException e){
                System.err.println(e);
            }
        }
    }

//    public class CommandHandler implements Runnable {
//        // Handle incoming commands
//
//        public CommandHandler(Socket socket) {this.socket = socket;}
//
//        public void run(){
//
//        }
//    }


    public static void main(String[] args) throws Exception {
        Node n = new Node(4444, 1, null);
        n.begin();
//        Thread t = new Thread(n);
//        Thread t = new Thread(new Node(4444, 1, null);)
//        t.run();
//        t.start();
        System.out.println("MADE IT HERE");
        Scanner scan = new Scanner(System.in);
        String com = scan.nextLine();
        while(!com.equals("quit")){
            n.takeCommand(com);
            com = scan.nextLine();
        }
        scan.close();
//        t.join();
    }
}