import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.MergeView;
import org.jgroups.View;

import java.util.List;
import java.util.Random;

public class MergeHandler extends Thread{
    private JChannel ch;
    private MergeView view;
    private int random;

    public MergeHandler(JChannel ch, MergeView view) {
        this.ch = ch;
        this.view = view;
        this.random = new Random().nextInt(2);
    }


    public void run() {
        List<View> subgroups = view.getSubgroups();
        View tmp_view = subgroups.get(random);
        Address local_addr = ch.getAddress();
        if(!tmp_view.getMembers().contains(local_addr)) {
            System.out.println("Not member of the partition (" + tmp_view + "), will re-acquire the state");
            try {
                ch.getState(local_addr, 100000);
            }
            catch(Exception ex) {
                System.err.println("Get state fail");
            }
        }
        else {
            System.out.println("Member of the partition (" + tmp_view + "), will do nothing");
        }
    }
}
