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

    private void createFile(String fname, int nodeID) {
        if (!tokens.containsKey(fname)) {
            Token t = new Token(fname, nodeID);
            tokens.put(fname, t);
            System.out.println("Created file: "+fname);
            relayToNeighbors("NEW", fname);
        }
    }

    private void deleteFile(String fname) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.remove(fname);
            System.out.println("Deleted file: "+fname);
            relayToNeighbors("DEL", fname);
        }
    }

    private void appendFile(String fname, String toAdd) {
        if (tokens.containsKey(fname)) {
//            Token t = tokens.remove(fname);
            Token t = tokens.get(fname);
            t.setInUse(true);
            tokens.put(fname, t);
            t.appendContents(toAdd);
            t.setInUse(false);
            tokens.put(fname, t);
            System.out.println("Appended to file: "+fname);
        }
    }

    public void readFile(String fname) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.get(fname);
            t.setInUse(true);
            tokens.put(fname, t);
            System.out.println("Reading "+fname+":");
            System.out.println(tokens.get(fname).getContents());
            t.setInUse(false);
            tokens.put(fname, t);
        }
    }

    private void assignToken(String fname) {
        Token t = tokens.get(fname);
        if (t.getHolder() == ID && !t.getInUse() && !t.isReqQEmpty()) {
            t.setHolder(t.deq());
            t.setAsked(false);
            if (t.getHolder() == ID) {
                t.setInUse(true);
                tokens.put(fname, t);
            }
            else {
                String msg = MessageSender.formatMsg("TOK", t.getHolder(), t.getContents(), null);
                MessageSender.sendMsg(neighbors.get(t.getHolder()).addr, neighbors.get(t.getHolder()).port, msg);
            }
        }
    }

    private void sendRequest(String fname) {
        Token t = tokens.get(fname);
        if (t.getHolder() != ID && !t.isReqQEmpty() && !t.getAsked()) {
            String msg = MessageSender.formatMsg("REQ", ID, fname, null);
        }
    }

    // Add node to token request queue. Can be used when another
    // node requests the token or when this node wants requests
    // the token.
    private void onReqReceipt(String fname, int nodeID) {
        Token t = Node.this.tokens.get(fname);
        t.request(nodeID);
        tokens.put(fname, t);
        assignToken(fname);
        sendRequest(fname);
    }

    private void relayToNeighbors(String command, String fname){
        String msg = MessageSender.formatMsg(command, ID, fname, null);
        for(Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
            String addr = entry.getValue().addr;
            int port = entry.getValue().port;
            MessageSender.sendMsg(addr, port, msg);
        }
    }

//    private void relayToNeighbors(String command, int nodeID, String fname){
//        String msg = MessageSender.formatMsg(command, nodeID, fname, null);
//        for(Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
//            String addr = entry.getValue().addr;
//            int port = entry.getValue().port;
//            MessageSender.sendMsg(addr, port, msg);
//        }
//    }

    private String[] parseCommand(String com){
        return com.split("\\s",3);
    }

    private void runCommand(String command, String fname, String contents){
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

        private String[] parseMsg(String msg){ return msg.split("\\|",4); }

        private void onTokReceipt(String fname, String data) {
            Token t = tokens.get(fname);
            t.setContents(data);
            t.setHolder(ID);
            tokens.put(fname, t);
        }

        private void handleMsg(String msg) {
            String[] m = parseMsg(msg);
            switch (m[0]){
                case "NEW":
                    System.out.println("Creating "+m[2]+" file");
                    Node.this.createFile(m[2], Integer.parseInt(m[1]));
                    break;
                case "DEL":
                    System.out.println("Deleting file");
                    Node.this.deleteFile(m[2]);
                    Node.this.relayToNeighbors("DEL", m[2]);
                    break;
                case "REQ":
                    System.out.println("Deleting file");
                    Node.this.onReqReceipt(m[2], Integer.parseInt(m[1]));
                    break;
                case "TOK":
                    System.out.println("Deleting file");
                    onTokReceipt(m[2], m[3]);
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
                System.out.println(msg);


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
//        String msg = MessageSender.formatMsg("TOK", 1, "FILENAME", "asdflkjasdflkj");
////        String msg = "TOK|2|FILENAME|asdlkfjasdoif|jwef";
//        String[] parsed =  msg.split("\\|",4);
//        System.out.println(parsed[0]);
//        System.out.println(parsed[1]);
//        System.out.println(parsed[2]);
//        System.out.println(parsed[3]);
//
//        msg = MessageSender.formatMsg("NEW", 2, "FILENAME", null);
////        msg = "NEW|2|FILENAME|";
//        parsed =  msg.split("\\|");
//        System.out.println(parsed[0]);
//        System.out.println(parsed[1]);
//        System.out.println(parsed[2]);
////
//        System.out.println(parsed.length);

//        int id = Integer.parseInt(args[0]);
//        HashMap<Integer, AddrPair> temp = parseConfigFile(args[2]);
//        Node n = new Node(temp.get(id).port, id);
//        n.initializeNeighbors(args[1], temp);
//
//        n.begin();
//
//        String msg = MessageSender.formatMsg("REQ", 2, "FILENAME");
//        MessageSender.sendMsg("localhost", 4444, msg);

//        Scanner scan = new Scanner(System.in);
//        String com = scan.nextLine();
//        while(!com.equals("quit")){
//            n.takeCommand(com);
//            com = scan.nextLine();
//        }
//        scan.close();
//        if(com.equals("quit")){
//            System.exit(0);
//        }
    }
}