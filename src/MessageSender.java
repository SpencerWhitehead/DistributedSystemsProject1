import java.io.*;
import java.net.Socket;

/**
 * Created by spencerwhitehead on 10/13/16.
 */
public class MessageSender {

    public MessageSender() {}

    public static void sendMsg(String senderID, String receiverIP, int receiverPort, String msg){
        try {
            Socket socket = new Socket(receiverIP, receiverPort);
            PrintStream os = new PrintStream(socket.getOutputStream());
            os.println(msg);
        }
        catch (IOException e) {
            System.err.println(e);
        }
    }

    public static void sendToken(String senderID, String receiverIP, int receiverPort, String fname){
        try {
            Socket socket = new Socket(receiverIP, receiverPort);
            File f = new File(fname);
            long len = f.length();
            byte[] fbytes = new byte[16*1024];
            InputStream is = new FileInputStream(f);
            OutputStream os = socket.getOutputStream();
            int count;
            while ((count = is.read(fbytes)) > 0) {
                os.write(fbytes, 0, count);
            }
            os.close();
            is.close();
            socket.close();
        }
        catch (IOException e) {
            System.err.println(e);
        }
    }
}
