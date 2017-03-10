package io;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.Receiver;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class JGroupsMessage {

    public static void main(String[] args) throws Exception {

        UUID id = UUID.randomUUID();
        System.out.println("I am " + id);

        //init Jgroups, setting up the jgroups cluster
        JChannel ch = new JChannel(JGroupsMessage.class.getResourceAsStream("/jgroups.xml"));
        ch.setDiscardOwnMessages(true);
        ch.connect("photon-cluster");

        //recieve messages, won't receive from local node, only remote ones.
        ch.receiver(msg -> {
            System.out.println("GOT DATA " + msg.getObject().toString());
        });

        //send messages. Probably use Muon Codecs for encoding here so we can switch to Avro at some point in the future
        while(true) {
            Map data = new HashMap();
            data.put("FROM", id);
            ch.send(new Message(null, data));
            Thread.sleep(2000);
        }

    }
}
