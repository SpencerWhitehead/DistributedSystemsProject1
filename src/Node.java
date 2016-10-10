import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;

public class Node implements Runnable {
    int portNum;	//Port Number on which the Node.java will be Listening to accept connection
    int ID;	        //NodeID of the node
    HashMap neighbors = new HashMap(); // Neighbor node IDs and IP addresses (nodeID, IP)
    HashMap holders = new HashMap(); // Holders of each file (filename, holderID)
    HashMap asked = new HashMap(); // Map to store which files have been requested (filename, asked)
    HashMap state = new HashMap(); // Map to store which files are being used (filename, inUse)

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
                    System.out.println("Appending "+contents+" to file");
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
        System.out.println("Parsed Command");
//        System.out.println(com[0]+" "+com[1]);
        System.out.println(com[0]);
        if(com.length == 2){
            runCommand(com[0], com[1], null);
        }
        else if(com.length == 3){
            runCommand(com[0], com[1], com[2]);
        }
    }

    public void run(){
        System.out.println("HELLO MY NODE HAS STARTED");
    }

    public static void main(String[] args) throws Exception {
        Node n = new Node(4444, 1, null);
        Scanner scan = new Scanner(System.in);
        String com = scan.nextLine();
        while(!com.equals("quit")){
            n.takeCommand(com);
            com = scan.nextLine();
        }
    }
}