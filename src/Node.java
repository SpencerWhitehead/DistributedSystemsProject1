/**
 * Created by spencerwhitehead on 10/10/16.
 */

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



//*** for an inner class to use the methods of an outer class. Use Node.this.createFile().

public class Node {
    private int portNum;	//Port Number on which the Node.java will be Listening to accept connection
    private int ID;	        //NodeID of the node
    private HashMap<Integer, AddrPair> neighbors = new HashMap<>(); // Neighbor node IDs, IP addresses, and port (nodeID, (IP,port))
//    private HashMap<Integer, String> neighbors = new HashMap<>(); // Neighbor node IDs and IP addresses (nodeID, IP)
//    private HashMap<String, Integer> holders = new HashMap(); // Holders of each file (filename, holderID)
//    private HashMap<String, Boolean> asked = new HashMap(); // Map to store which files have been requested (filename, asked)
//    private HashMap<String, Boolean> state = new HashMap(); // Map to store which files are being used by this node (filename, inUse)
//    private HashMap<String, Queue<Integer>> reqQ = new HashMap(); // Map to store request queue for each file
    private ConcurrentHashMap<String, Token> tokens = new ConcurrentHashMap<>();

    public Node(int port, int ident) {
        this.portNum = port;
        this.ID = ident;
    }

//    public void initializeNeighbors(HashMap<Integer, AddrPair> addrs)

    public void createFile(String fname, int nodeID) {
        if (!tokens.containsKey(fname)) {
            Token t = new Token(fname, nodeID);
            t.setInUse(true);
            tokens.put(fname, t);
            tokens.get(fname).setInUse(false);
            System.out.println("Created file: "+fname);
        }
    }

    public void deleteFile(String fname) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.remove(fname);
            System.out.println("Deleted file: "+fname);
        }
    }

    public void appendFile(String fname, String toAdd) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.remove(fname);
            t.appendContents(toAdd);
            tokens.put(fname, t);
            System.out.println("Appended to file: "+fname);
        }
    }

    public void readFile(String fname) {
        if (tokens.containsKey(fname)) {
            System.out.println("Reading "+fname+":");
            System.out.println(tokens.get(fname).getContents());
        }
    }

    private void assignToken(String fname) {
        Token t = tokens.get(fname);
        if (t.getHolder() == ID && !t.getInUse() && !t.isReqQEmpty()) {
            t.setHolder(t.deq());
            t.setAsked(false);
            if (t.getHolder() == ID) {
                t.setInUse(true);
            }
            else {
                String msg = MessageSender.formatMsg("TOK", t.getHolder(), t.getContents());
                MessageSender.sendMsg(neighbors.get(t.getHolder()).addr, neighbors.get(t.getHolder()).port, msg);
            }
        }
    }

    private void sendRequest(String fname) {
        Token t = tokens.get(fname);
        if (t.getHolder() != ID && !t.isReqQEmpty() && !t.getAsked()) {
            String msg = MessageSender.formatMsg("REQ", ID, fname);
        }
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

        public String[] parseMsg(String msg){ return msg.split("|",3); }

        public void handleMsg(String msg) {
            String[] m = parseMsg(msg);
            switch (m[0]){
                case "NEW":
                    System.out.println("Creating "+m[2]+" file");
                    Node.this.createFile(m[1], Integer.parseInt(m[2]));
                    break;
                case "DEL":
                    System.out.println("Deleting file");
                    Node.this.deleteFile(m[2]);
                    break;
                case "REQ":
                    System.out.println("Deleting file");
                    Node.this.deleteFile(m[2]);
                    break;
                case "TOK":
                    System.out.println("Deleting file");
                    Node.this.deleteFile(m[2]);
                    break;
                default:
                    System.out.println("Invalid message: "+msg);
                    break;
            }
        }

        public void run(){
            try {
                System.out.println("HANDLING CONNECTION!");

                is = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msg = is.readLine();


//                os = new PrintStream(socket.getOutputStream());
//
//                while(true){
//                    String s = is.readLine();
//                    System.out.println(s);
//                    os.println("GOT IT!");
//                    System.out.println("SENT MESSAGE!");
//                    break;
//                }
                is.close();
//                os.close();
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


    public static HashMap<Integer, AddrPair> parseConfigFile(String fname) {
        HashMap<Integer, AddrPair> addrs = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fname))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] s = line.split("\\s", 3);
                AddrPair t = new AddrPair(s[1], Integer.parseInt(s[2]));
                addrs.put(Integer.parseInt(s[0]), t);
            }
        }
        catch (IOException e) {
            System.err.println(e);
        }
        return  addrs;
    }

    public void initializeNeighbors(String fname, HashMap<Integer, AddrPair> addrs) {
        HashMap<Integer, AddrPair> adj = new HashMap<>();
        try (BufferedReader br = new BufferedReader(new FileReader(fname))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] s = line.split(",");
                String temp1 = s[0].replace("(","");
                String temp2 = s[1].replace(")","");
                int nodeID1 = Integer.parseInt(temp1);
                int nodeID2 = Integer.parseInt(temp2);
                if(nodeID1 == ID || nodeID2 == ID) {
                    int neigh = nodeID1 != ID ? nodeID1 : nodeID2;
                    adj.put(neigh, addrs.get(neigh));
                }
            }
        }
        catch (IOException e) {
            System.err.println(e);
        }
        neighbors = adj;
    }

    public static void main(String[] args) throws Exception {
        if(args.length != 3) {
            System.out.println("Arguments: <current node id> <tree file> <configuration file>");
            System.exit(0);
        }

        int id = Integer.parseInt(args[0]);
        HashMap<Integer, AddrPair> temp = parseConfigFile(args[2]);
        Node n = new Node(temp.get(id).port, id);
        n.initializeNeighbors(args[1], temp);

        n.begin();
        System.out.println("MADE IT HERE");
        Scanner scan = new Scanner(System.in);
        String com = scan.nextLine();
        while(!com.equals("quit")){
            n.takeCommand(com);
            com = scan.nextLine();
        }
        scan.close();
        if(com.equals("quit")){
            System.exit(0);
        }
    }
}