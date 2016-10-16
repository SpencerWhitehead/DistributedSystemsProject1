/**
 * Created by spencerwhitehead on 10/10/16.
 */

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;



//*** for an inner class to use the methods of an outer class. Use Node.this.createFile().

public class Node {
    private int portNum;	//Port Number on which the Node.java will be Listening to accept connection
    private int ID;	        //NodeID of the node
    private HashMap<Integer, AddrPair> neighbors = new HashMap<>(); // Neighbor node IDs, IP addresses, and port (nodeID, (IP,port))
    private ConcurrentHashMap<String, Token> tokens = new ConcurrentHashMap<>();
    private ConcurrentHashMap<String, Queue<String[]>> commands = new ConcurrentHashMap<>(); // File name to commands to run

    public Node(int port, int ident) {
        this.portNum = port;
        this.ID = ident;
    }

    private void createFile(String fname, int nodeID) {
        if (!tokens.containsKey(fname)) {
            Token t = new Token(fname, nodeID);
            tokens.put(fname, t);
            System.out.println("\tCreated file: "+fname);
            StringBuilder s = new StringBuilder();
            s.append("\tNumber of tokens: ");
            s.append(tokens.size());
            System.out.println(s.toString());
            relayToNeighbors("NEW", fname, nodeID);
        }
        else {
            System.err.println("\tError: file already exists, "+fname);
        }
    }

    private void deleteFile(String fname, int nodeID) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.remove(fname);
            System.out.println("\tDeleted file: "+fname);
            StringBuilder s = new StringBuilder();
            s.append("\tNumber of tokens: ");
            s.append(tokens.size());
            System.out.println(s.toString());
            relayToNeighbors("DEL", fname, nodeID);
        }
        else {
            System.err.println("\tError: no such file, "+fname);
        }
    }

    private void appendFile(String fname, String toAdd) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.get(fname);
            t.appendContents(toAdd);
            tokens.put(fname, t);
            System.out.println("\tAppended to file: "+fname);
        }
        else {
            System.err.println("\tError: no such file, "+fname);
        }
    }

    private void readFile(String fname) {
        if (tokens.containsKey(fname)) {
            Token t = tokens.get(fname);
            System.out.println("\tReading "+fname+":");
            System.out.println("\t\t"+tokens.get(fname).getContents());
        }
        else {
            System.err.println("\tError: no such file, "+fname);
        }
    }

    private void performCommands(Queue<String[]> comQ) {
        while(!comQ.isEmpty()) {
            String[] currentCom = comQ.poll();
            if(currentCom.length == 2){
                runCommand(currentCom[0], currentCom[1], null);
            }
            else if(currentCom.length == 3){
                runCommand(currentCom[0], currentCom[1], currentCom[2]);
            }
        }
    }

    private void assignToken(String fname) {
        Token t = tokens.get(fname);
        if (t.getHolder() == ID && !t.getInUse() && !t.isReqQEmpty()) {
            t.setHolder(t.deq());
            t.setAsked(false);
            tokens.put(fname, t);
            if (t.getHolder() == ID) {
                t.setInUse(true);
                tokens.put(fname, t);

                // Do something with the token
                System.out.println("\tGot "+fname+" token. Running commands...");
                Queue<String[]> comQ = commands.remove(fname);
                performCommands(comQ);
            }
            else {
                String msg = MessageSender.formatMsg("TOK", ID, fname, t.getContents());
                MessageSender.sendMsg(neighbors.get(t.getHolder()).addr, neighbors.get(t.getHolder()).port, msg);
                t.releaseContents();
                tokens.put(fname, t);
            }
        }
    }

    private void sendRequest(String fname) {
        if(tokens.containsKey(fname)) {
            Token t = tokens.get(fname);
            if (t.getHolder() != ID && !t.isReqQEmpty() && !t.getAsked()) {
                System.out.println("\tSending request for " + fname);
                String msg = MessageSender.formatMsg("REQ", ID, fname, null);
                MessageSender.sendMsg(neighbors.get(t.getHolder()).addr, neighbors.get(t.getHolder()).port, msg);
                t.setAsked(true);
                tokens.put(fname, t);
            }
        }
    }

    // Add node to token request queue. Can be used when another
    // node requests the token or when this node wants requests
    // the token.
    private void onReq(String fname, int nodeID) {
        if(tokens.containsKey(fname)) {
            System.out.println("\tRequesting "+fname);
            Token t = tokens.get(fname);
            t.request(nodeID);
            tokens.put(fname, t);
            assignToken(fname);
            sendRequest(fname);

            if(tokens.containsKey(fname)) {
                t = tokens.get(fname);
                if (t.getInUse()) {
                    onRelease(fname);
                }
            }
        }
        else {
            System.err.println("\tError: no such file, "+fname);
        }
    }

    private void onRelease(String fname) {
        Token t = tokens.get(fname);
        t.setInUse(false);
        tokens.put(fname, t);
        assignToken(fname);
        sendRequest(fname);
        System.out.println("\tReleased "+fname);
    }

    private void relayToNeighbors(String command, String fname, int prevID){
        String msg = MessageSender.formatMsg(command, ID, fname, null);
        for(Map.Entry<Integer, AddrPair> entry : neighbors.entrySet()) {
            if (!entry.getKey().equals(prevID)) {
                StringBuilder s = new StringBuilder();
                s.append("\tNotifying node ");
                s.append(entry.getKey());
                System.out.println(s.toString());
                String addr = entry.getValue().addr;
                int port = entry.getValue().port;
                MessageSender.sendMsg(addr, port, msg);
            }
        }
    }

    private String[] parseCommand(String com){
        return com.split("\\s",3);
    }

    private void runCommand(String command, String fname, String contents){
        switch (command){
            case "create":
                createFile(fname, ID);
                break;
            case "delete":
                deleteFile(fname, ID);
                break;
            case "read":
                readFile(fname);
                break;
            case "append":
                if(contents != null) {
                    appendFile(fname, contents);
                }
                break;
            default:
                System.err.println("\tInvalid command: "+command);
                break;
        }
    }

    public void takeCommand(String command){
        String[] com = parseCommand(command);
        if(com.length == 2 || com.length == 3){
            Thread commandThread = new Thread(new CommandHandler(command));
            commandThread.run();
        }
        else {
            System.err.println("\tInvalid command: "+command);
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

    public class ConnectHandler implements Runnable {
        // Handle socket connection and receive and send messages
        private Socket socket = null;
        private BufferedReader is = null;

        public ConnectHandler(Socket socket) {this.socket = socket;}

        private String[] parseMsg(String msg){ return msg.split("\\|",4); }

        private void onTokReceipt(String fname, String data) {
            Token t = tokens.get(fname);
            t.setContents(data);
            t.setHolder(ID);
            tokens.put(fname, t);
            Node.this.assignToken(fname);
            Node.this.sendRequest(fname);
        }

        private void handleMsg(String msg) {
            String[] m = parseMsg(msg);
            switch (m[0]){
                case "NEW":
                    if(!Node.this.tokens.containsKey(m[2])) {
                        System.out.println("\tCreating file: "+m[2]);
                        Node.this.createFile(m[2], Integer.parseInt(m[1]));
                    }
                    break;
                case "DEL":
                    if(Node.this.tokens.containsKey(m[2])) {
                        System.out.println("\tDeleting file: "+m[2]);
                        Node.this.deleteFile(m[2], Integer.parseInt(m[1]));
                    }
                    break;
                case "REQ":
                    System.out.println("\tReceived request for file: "+m[2]);
                    Node.this.onReq(m[2], Integer.parseInt(m[1]));
                    break;
                case "TOK":
                    System.out.println("\tReceived token: "+m[2]);
                    onTokReceipt(m[2], m[3]);
                    break;
                default:
                    System.err.println("\tInvalid message: "+msg);
                    break;
            }
        }

        public void run(){
            try {
                is = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                String msg = is.readLine();
                System.out.println("\tReceived: "+msg);
                handleMsg(msg);
                is.close();
                socket.close();
            }
            catch (IOException e){
                System.err.println(e);
            }
        }
    }

    public class CommandHandler implements Runnable {
        // Handle incoming commands
        String command;

        public CommandHandler(String newCom) {command = newCom;}

        public void addCommand(String fname, String[] com) {
            if (Node.this.commands.containsKey(com)) {
                Queue<String[]> q = Node.this.commands.get(com);
                q.add(com);
                Node.this.commands.put(fname, q);
            }
            else {
                Queue<String[]> q = new ConcurrentLinkedQueue<>();
                q.add(com);
                Node.this.commands.put(fname, q);
            }
        }

        public void run(){
            String[] com = parseCommand(command);
            if(com.length == 2 || com.length == 3){
                if(com.length == 2 && com[0].equals("create")) {
                    runCommand(com[0], com[1], null);
                }
                else {
                    addCommand(com[1], com);
                    Node.this.onReq(com[1], Node.this.ID);
                }
            }
            else {
                System.err.println("\tInvalid command: "+command);
            }
        }
    }


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