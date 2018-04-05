import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.protocols.pbcast.STATE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;


public class DistributedMap implements SimpleStringMap{
    private Map<String, String> distrMap = new HashMap<>();
    private JChannel mapChannel;

    public DistributedMap() throws Exception {
        mapChannel = new JChannel(false);

        ProtocolStack stack=new ProtocolStack();
        mapChannel.setProtocolStack(stack);
        stack.addProtocol(new UDP().setValue("mcast_group_addr", InetAddress.getByName("230.0.0.240")))
                .addProtocol(new PING())
                .addProtocol(new MERGE3())
                .addProtocol(new FD_SOCK())
                .addProtocol(new FD_ALL().setValue("timeout", 12000).setValue("interval", 3000))
                //.addProtocol(new VERIFY_SUSPECT())
                .addProtocol(new BARRIER())
                .addProtocol(new NAKACK2())
                .addProtocol(new UNICAST3())
                .addProtocol(new STABLE())
                .addProtocol(new GMS())
                .addProtocol(new UFC())
                .addProtocol(new MFC())
                .addProtocol(new FRAG2())
                .addProtocol(new STATE());

        stack.init();

        mapChannel.setReceiver(new ReceiverAdapter(){
            @Override
            public void viewAccepted(View view) {
                System.out.println("view update");
                if(view instanceof MergeView){
                    System.out.println("mergeview detected");
                    new MergeHandler(mapChannel, (MergeView)view).start();
                }
            }

            public void receive(Message msg) {
                String txt = (String)msg.getObject();
                String[] command = txt.split(";;");
                switch(command[0]) {
                    case "p":
                        distrMap.put(command[1], command[2]);
                        break;
                    case "r":
                        distrMap.remove(command[1]);
                        break;
                    default:
                        System.err.println("Received unknown command");
                }
            }

            public void getState(OutputStream outputStream) throws Exception {
                synchronized (distrMap){
                    Util.objectToStream(distrMap, new DataOutputStream(outputStream));
                }
            }

            public void setState(InputStream inputStream) throws Exception {
                HashMap<String, String> hm = (HashMap<String, String>)Util.objectFromStream(new DataInputStream(inputStream));
                synchronized (distrMap){
                    distrMap.clear();
                    distrMap.putAll(hm);
                }
                System.out.println(distrMap.size() + " entries got from group:" + distrMap.toString());
            }
        });

        mapChannel.connect("DHTCluster");

        mapChannel.getState(null, 100000);
    }

    @Override
    public boolean containsKey(String key) {
        return distrMap.containsKey(key);
    }

    @Override
    public String get(String key) {
        return distrMap.get(key);
    }

    @Override
    public String put(String key, String value) throws Exception {
        mapChannel.send(new Message(null, null, "p;;" + key + ";;" + value));
        return null;
    }

    @Override
    public String remove(String key) throws Exception {
        mapChannel.send(new Message(null, null, "r;;" + key));
        return null;
    }

    public void finish(){
        mapChannel.close();
    }
}
