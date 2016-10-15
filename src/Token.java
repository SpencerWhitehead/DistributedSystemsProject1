import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Created by spencerwhitehead on 10/13/16.
 */
public class Token {
    private String fname;
    private int holder;
    private boolean asked = false;
    private boolean inUse = false;
    private Queue<Integer> reqQ = new ConcurrentLinkedQueue<>();
    String contents = "";

    public Token(String f, int hold) {
        this.fname = f;
        this.holder = hold;
    }

    public String getFname() {return fname;}

    public int getHolder() {return holder;}

    public boolean getAsked() {return asked;}

    public boolean getInUse() {return inUse;}

    public String getContents() {return contents;}

    public boolean isReqQEmpty() {return reqQ.isEmpty();}

    public void setInUse(boolean use) {inUse = use;}

    public void setAsked(boolean ask) {asked = ask;}

    public void setHolder(int hold) {holder = hold;}

    public void setContents(String data) {contents = data;}

    public void request(int nodeID) {reqQ.add(nodeID);}

    public Integer deq() {return reqQ.poll();}

    public void appendContents(String toAppend) {
        StringBuilder s = new StringBuilder();
        s.append(contents);
        s.append(toAppend);
        contents = s.toString();
    }

    public void releaseContents() {contents = "";}
}
