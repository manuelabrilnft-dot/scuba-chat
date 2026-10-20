import client.*;

import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;

/**
* This is just some example code to show you how to interact 
* with the server using the provided client and two queues.
* Feel free to modify this code in any way you like!
*/

// TODO: Check if to replace knownIds value by own sourceId when own sourceId sends (meaning it moved into range or the initial heartbeat packet was lost and now we know about it through own node)

public class MyProtocol {

    private static final int MAX_ID = 8;

    // The host to connect to. Set this to localhost when using the audio interface tool.
    private static String SERVER_IP = "netsys.ewi.utwente.nl"; //"127.0.0.1";
    // The port to connect to. 8954 for the simulation server.
    private static int SERVER_PORT = 8954;
    // The frequency to use.
    private static int frequency = 2600;
    // View the simulator at https://netsys.ewi.utwente.nl/integrationproject/
    // The token you received for your frequency range
    String token = "java-21-E5495E4CE445DD33A2";

    private BlockingQueue<Message> receivedQueue;
    private BlockingQueue<Message> sendingQueue;

    // This queue is needed as we don't want to put everything in the sendingQueue, instead we want
    // to put everything in the localQueue so we can only add to the sendingQueue only the priority
    // packets such as (ACKs, Heartbeats, Token Passing)
    private BlockingQueue<Message> localQueue;

    private boolean hasToken = false;
    private boolean ackReceived = false;
    private boolean isSending = false;
    private boolean tokenAckReceived = false;
    private int tokenRetransmissionCount = 0;


    private Map<String, Integer> parameters;
    private Map<Integer, Integer> knownIds;
    private Map<Integer, Long> heartbeatHistory;
    private Set<Integer> lockedIds;


    public MyProtocol(String server_ip, int server_port, int frequency) {
        receivedQueue = new LinkedBlockingQueue<Message>();
        sendingQueue = new LinkedBlockingQueue<Message>();

        knownIds = new ConcurrentHashMap<>();
        heartbeatHistory = new ConcurrentHashMap<>();
        lockedIds = ConcurrentHashMap.newKeySet();

        parameters = new ConcurrentHashMap<>();
        parameters.put("sourceId", new Random().nextInt(1, MAX_ID));
        parameters.put("bitmap", 1 << (parameters.get("sourceId") - 1));
        parameters.put("nonce", new Random().nextInt(14));

        //TODO: If sending queue is empty, eliberate the token straight away
        new Client(SERVER_IP, SERVER_PORT, frequency, token, receivedQueue, sendingQueue); // Give the client the Queues to use
        new HeartbeatThread().start();
        new HeartbeatHistoryThread().start();
        new TokenThread().start();
        new EliberateTokenThread().start();
        new pushToSendingQueueThread().start();
        new receiveThread(receivedQueue).start(); // Start thread to handle received messages!

        // handle sending from stdin from this thread.
        try {
            ByteBuffer temp = ByteBuffer.allocate(1024);
            int read = 0;
            int new_line_offset = 0;
            while (true) {
                read = System.in.read(temp.array()); // Get data from stdin, hit enter to send!
                if (read > 0) {
                    if (temp.get(read-1) == '\n' || temp.get(read-1) == '\r' ) new_line_offset = 1; //Check if last char is a return or newline so we can strip it
                    if (read > 1 && (temp.get(read-2) == '\n' || temp.get(read-2) == '\r') ) new_line_offset = 2; //Check if second to last char is a return or newline so we can strip it

                    /*
                     We can use a for loop in order to add all the messages to the sendingQueue
                     */

                    // fragment count -> divide the payload data size by 29 (32 - headerSize (2) - paddingByte (1))
                    int dataSize = read - new_line_offset;

                    for (int i = 0; i < dataSize ; i += 29) {
                        int fragmentSize = Math.min(29, dataSize - i);
                        int fragmentSequence = i / 29;
                        boolean isLast = (i + 29 >= dataSize);

                        ByteBuffer toSend = ByteBuffer.allocate(32); // copy data without newline / returns
                        Message msg;
                        Header header;
                        // ↓ FIX THIS SHIT AFTER HEADER CHANGES! ↓
                        header = new PayloadHeader(false,parameters.get("sourceId"), 1, isLast, 1, fragmentSequence);
                        toSend.putShort((short) header.assemble());
                        toSend.put( temp.array() , i, fragmentSize); // enter data without newline / returns

                        // Payload can be at most 29
                        byte[] padding = new byte[30 - fragmentSize];
                        padPayload(padding);
                        toSend.put(padding, 0, padding.length);
                        msg = new Message(MessageType.DATA, toSend);
                        localQueue.put(msg);
                    }

                }
            }
        } catch (InterruptedException e){
            System.exit(2);
        } catch (IOException e){
            System.exit(2);
        }        
    }

    public void padPayload(byte[] padding) {
        // byte[] is zero-initialized by new; just place the sentinel.
        if (padding.length > 0) padding[0] = (byte) 128;
    }

    public static void main(String args[]) {
        if(args.length > 0){
            frequency = Integer.parseInt(args[0]);
        }
        new MyProtocol(SERVER_IP, SERVER_PORT, frequency);
    }

    private boolean isNewcomer(int bitmap) {
        int counter = 0;
        for (int i = 0; i < MAX_ID - 1; i++) {
            if (((bitmap >>> i) & 1) == 1) {
                counter += 1;
            }
        }
        return counter == 1;
    }

    private int updateKnownIds(int local, int incoming, int localId, int incomingId) {
        int newBitmap = 0;
        Set<Integer> toRemove = new HashSet<>();
        for (int i = 0; i < 7; i++) {
            if (i + 1 == localId) continue; 
            int localBit = (local >>> i) & 0x01;
            int incomingBit = (incoming >>> i) & 0x01;
            // i + 1 = next hop
            if (localBit == 0 && incomingBit == 1 && !lockedIds.contains(i + 1)) {
                knownIds.put(i + 1, incomingId);
                LockThread lockThread = new LockThread();
                lockThread.boot(i + 1);
                lockThread.start();
                newBitmap = newBitmap | (incomingBit << i);
            } else if (localBit == 1 && incomingBit == 0  && !lockedIds.contains(i + 1)) {
                Integer incomingHop = knownIds.get(i + 1);
                if (incomingHop != null && incomingHop.intValue() == incomingId) {
                    for (Integer knownId : knownIds.keySet()) {
                        if (knownIds.get(knownId).intValue() == (i + 1)) {
                            toRemove.add(knownId);
                        }
                    }
                    toRemove.add(i + 1);
                    LockThread lockThread = new LockThread();
                    lockThread.boot(i + 1);
                    lockThread.start();
                } else {
                    newBitmap = newBitmap | (localBit << i);
                }
            } else {
                newBitmap = newBitmap | (localBit << i);
            }
        }
        for (Integer id : toRemove) {
            knownIds.remove(id);
        }
        newBitmap = (1 << (localId - 1)) | newBitmap;
        return newBitmap;
    }

    private void randomId(int bitmap, Map<String, Integer> parameters, boolean amNewcomer) {
        System.out.println("RANDOM ID");
        if (bitmap == 0b01111111) System.exit(2);
        int rand = new Random().nextInt(1, MAX_ID);
        while (((bitmap >>> (rand - 1)) & 1) == 1) {
            rand = new Random().nextInt(1, MAX_ID);
        }
        parameters.put("sourceId", rand);
        parameters.put("nonce", new Random().nextInt(14));
        bitmap = bitmap | (1 << (rand - 1));
        parameters.put("bitmap", bitmap);
    }

    private void sendShort(Header header){
        ByteBuffer toSend = ByteBuffer.allocate(2);
        Message msg = new Message(MessageType.DATA_SHORT, toSend);
        toSend.putShort((short) header.assemble());
        try {
            sendingQueue.put(msg);
        } catch (InterruptedException e){
            System.exit(2);
        }
    }

    private void sendHeartbeat(HeartbeatHeader header, boolean isCollision) {
        header.setSourceId(parameters.get("sourceId"));
        header.setNonce(parameters.get("nonce"));
        header.isCollision(isCollision);
        header.setBitmap(parameters.get("bitmap"));
        System.out.println("OUT HB: " + Integer.toBinaryString(header.getBitmap()) + " " + Integer.toBinaryString(parameters.get("bitmap")));
        sendShort(header);
    }

    private int findNextTokenBearer(){
        List<Integer> list = new ArrayList<>(knownIds.keySet());
        Collections.sort(list);
        if(list.indexOf(parameters.get("sourceId")) == list.size() - 1){
            return list.getFirst();
        }else{
            return list.get(list.indexOf(parameters.get("sourceId")) + 1);
        }
    }

    private void transmitToken(){
        int dest = findNextTokenBearer();
        Header tokenHeader = new TokenHeader(parameters.get("sourceId"), 0xF, dest, knownIds.get(dest));
        sendShort(tokenHeader);
    }

    private class EliberateTokenThread extends Thread {
        public void run() {
            while(true) {
                if(hasToken && localQueue.size() == 0) {
                    ackReceived = true;
                }
            }
        }
    }

    private class TimeoutTokenThread extends Thread implements Utils.TimeoutEvent {
        public void run() {
            String timerKey = "ConditionCheck_" + parameters.get("sourceId");
            //TODO: Test the timeout
            Utils.Timeout.setTimeout(timerKey, null, 1000, this);
        }

        @Override
        public void onElapsed(Object data) {
            if(tokenRetransmissionCount == 4){
                ackReceived = true;
            }
            if (!tokenAckReceived && tokenRetransmissionCount < 4) {
                transmitToken();
                tokenRetransmissionCount++;
            }
        }
    }

    private class TokenThread extends Thread {
        public void run() {
            while (true) {
                if(ackReceived){
                    tokenAckReceived = false;
                    transmitToken();
                    new TimeoutTokenThread().start();
                    ackReceived = false;
                }
            }
        }
    }


    private class HeartbeatThread extends Thread {
        public void run() {
            while (true) {
                sendShort(new HeartbeatHeader(parameters.get("sourceId"), parameters.get("nonce"), false, parameters.get("bitmap")));
                if(ackReceived){
                    int dest = findNextTokenBearer();
                    Header tokenHeader = new TokenHeader(parameters.get("sourceId"), 0xF, dest, knownIds.get(dest));
                    sendShort(tokenHeader);
                    ackReceived = false;
                }
                System.out.println("Bitmap: " + parameters.get("bitmap"));
                try {
                    Thread.sleep(new Random().nextInt(2000,2500));
                } catch (InterruptedException e) {
                    System.exit(2);
                }
            }
        }
    }

    private void updateHeartbeat(int nodeId) {
        heartbeatHistory.put(nodeId, new Date().getTime());
        System.out.println("heartBeatHistory: " + heartbeatHistory.toString());
    }

    private class HeartbeatHistoryThread extends Thread {
        public void run() {
            while (true) {
                int localSourceId = parameters.get("sourceId");
                int localNonce = parameters.get("nonce");
                int localBitmap = parameters.get("bitmap");
                int finalBitmap = localBitmap;
                Set<Integer> toRemove = new HashSet<>();
                for (Integer knownId : knownIds.keySet()) {
                    long curTime = new Date().getTime();
                    int nextHop = knownIds.get(knownId);
                    Long lastHeartbeat = heartbeatHistory.get(nextHop);
                    if (lastHeartbeat == null) continue;
                    long timeDiff = curTime - lastHeartbeat;
                    if (timeDiff < 30000) {
                        continue;
                    }
                    // NEXTHOP GONE
                    for (Integer forgotten : knownIds.keySet()) {
                        if (knownIds.get(forgotten).intValue() == nextHop) {
                            toRemove.add(forgotten);
                        }
                    }
                    heartbeatHistory.remove(nextHop);
                }
                for (Integer id : toRemove) {
                    knownIds.remove(id);
                    int mask = 1 << (id - 1);
                    finalBitmap &= ~mask;
                }
                if (!toRemove.isEmpty()) {
                    parameters.put("bitmap", finalBitmap);
                    sendHeartbeat(new HeartbeatHeader(localSourceId, localNonce, false, finalBitmap), false);
                }
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    System.exit(2);
                }
            }
        }
    }

    private class LockThread extends Thread {
        private int toLock;

        public void boot(int toLock) {
            this.toLock = toLock;
        }

        public void run() {
            lockedIds.add(toLock);
            try {
                Thread.sleep(5000);
                lockedIds.remove(toLock);
                
            } catch (InterruptedException e) {
                System.exit(2);
            }
        }
    }

    private class pushToSendingQueueThread extends Thread implements Utils.TimeoutEvent {
        public void createTimeout(Object data, Header header) {
            if (header instanceof PayloadHeader) {
                String key = createTimeoutKey(header, false);

                Utils.Timeout.setTimeout(key, data, 2000, this);
            }
        }

        @Override
        public void onElapsed(Object data) {
            // Retransmission if timeout elapsed
            try {
                sendingQueue.put((Message) data);
                Header header = Header.parse(((Message) data).getData());
                createTimeout(data, header);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run() {
            // isSending is necessary to avoid this thread to send multiple times once we have the token
            while (true) {
                try {
                    if (hasToken && !isSending) {
                        // TODO: isSending must be set to false once our packets are acknowledged
                         isSending = true;

                         Message packet = localQueue.take();
                         sendingQueue.put(packet);
                         Header header = Header.parse(packet.getData());
                         createTimeout(packet, header);
                    }
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static String createTimeoutKey(Header header, boolean inverse) {
        int sequenceNumber = ((PayloadHeader) header).getSequenceNumber();
        int sourceId = header.getSourceId();
        int destId = ((PayloadHeader) header).getDestId();

        // Our key will have the following protocol
        // -> <sequenceNumber;srcId:destId>
        // We need this information so we can be able to correctly terminate
        // the timeout once we ack
        String key;
        // if inverse is true we want to switch the positions between sourceId and switch ID
        if (!inverse) {
            key = String.valueOf(sequenceNumber) + ';' + sourceId + ':' + destId;
        } else {
            key = String.valueOf(sequenceNumber) + ';' + destId + ':' + sourceId;
        }
        return key;
    }


    private class receiveThread extends Thread {
        private BlockingQueue<Message> receivedQueue;
        private Map<Integer, List<Message>> fragments = new HashMap<>();

        public receiveThread(BlockingQueue<Message> receivedQueue){
            super();
            this.receivedQueue = receivedQueue;
        }

        public void printByteBuffer(ByteBuffer bytes, int bytesLength){
            for(int i=0; i<bytesLength; i++){
                System.out.print( Byte.toString( bytes.get(i) ) + " " );
            }
            System.out.println();
        }

        public String retrieveByteString(ByteBuffer bytes, int bytesLength) {
            return new String(bytes.array(), 0, bytesLength);
        }

        public void printByteString(ByteBuffer bytes, int bytesLength) {
            System.out.println(retrieveByteString(bytes, bytesLength));
        }

        public ByteBuffer stripPadding(ByteBuffer data) {
            for (int i = data.limit() - 1; i >= data.position(); i--) {
                if (data.get(i) == (byte) 128) {
                    ByteBuffer result = data.duplicate();
                    result.limit(i);
                    return result;
                }
            }
            return data.duplicate();
        }

        public ByteBuffer stripHeader(ByteBuffer data) {
            return data.slice(0, 2);
        }

        public ByteBuffer getCleanData(ByteBuffer data) {
            return stripHeader(stripPadding(data));
        }

        public void orderMessagesBySequence(List<Message> messages) {
            messages.sort((msg1, msg2) -> {
                Header header1 = Header.parse(msg1.getData());
                Header header2 = Header.parse(msg2.getData());
                if (header1 instanceof PayloadHeader && header2 instanceof PayloadHeader) {
                    if (((PayloadHeader) header1).getSequenceNumber()
                            < ((PayloadHeader) header2).getSequenceNumber()) {
                        return -1;
                    }
                    if (((PayloadHeader) header1).getSequenceNumber()
                            > ((PayloadHeader) header2).getSequenceNumber()) {
                        return 1;
                    }
                }
                return 0;
            });
        }


        public void run() {
            while (true) {
                try {
                    Message m = receivedQueue.take();
                    if (m.getType() == MessageType.BUSY){
                        System.out.println("BUSY");
                    } else if (m.getType() == MessageType.FREE){
                        System.out.println("FREE");
                    } else if (m.getType() == MessageType.DATA){

                        // System.out.println(str);
                        Header header = Header.parse(m.getData());
                        if (header instanceof PayloadHeader) {
                            boolean isAck = header.isAck();

                            // Check if the message is not an ack and if the message is meant to be
                            // sent to us.
                            // -> create the ACK packet and send it.
                            // -> add the message to a Map so we can then join the payload together
                            //
                            // If the nextHop
                            // -> Forward the message to other nodes
                            if (!isAck && ((PayloadHeader) header).getDestId() == parameters.get("sourceId")) {

                                // We have to store multiple fragments per ID, therefore we need to
                                // use a function like computeIfAbsent so we can initialize the List
                                // before adding values to the Array List
                                fragments.computeIfAbsent(header.getSourceId(), k -> new ArrayList<>()).add(m);

                                if (((PayloadHeader) header).isLastFragment()) {
                                    List<Message> messageFragments = fragments.get(header.getSourceId());

                                    // Order the messages by the sequence
                                    orderMessagesBySequence(messageFragments);

                                    StringBuilder builder = new StringBuilder();
                                    // Loop through all the fragments and join them together
                                    for (Message fragment : messageFragments) {
                                        // Get only the payload data from the message
                                        ByteBuffer cleanData = getCleanData(fragment.getData());
                                        // Joining the message
                                        builder.append(retrieveByteString(cleanData, cleanData.array().length));
                                    }

                                    // Printing the message
                                    System.out.println(builder);

                                    // Removing the message from the Map
                                    fragments.remove(header.getSourceId());

                                    // Creation of ack message and putting in sendingQueue
                                    sendAck(header);
                                }
                            } else if (!isAck) {
                                int destId = ((PayloadHeader) header).getDestId();
                                int nextHopField = ((PayloadHeader) header).getNextHop();

                                // If we are the nextHop we should forward the message to the
                                //  next node in our routing table
                                if (nextHopField == parameters.get("destId")) {
                                    int nextHop = knownIds.getOrDefault(destId, -1);

                                    // forward the packet to the nextHop
                                    if (nextHop != -1) {

                                        // Copy m
                                        Message newMsg = new Message(m.getType(), Utils.byteBufferClone(m.getData()));

                                        // Change header
                                        ((PayloadHeader)header).setNextHop(nextHop);
                                        ByteBuffer newData = Utils.byteBufferClone(newMsg.getData());

                                        // recraft the packet
                                        stripHeader(newData);
                                        newData.position(0);
                                        newData.putShort((short) header.assemble());

                                        sendingQueue.put(newMsg);
                                    }
                                }

                                // Forward the message
                            }

                        } else {
                            // Heartbeat Header
                        }
                        System.out.print("DATA: ");
                        printByteBuffer( m.getData(), m.getData().capacity()); //Just print the data
                        printByteString(m.getData(), m.getData().capacity());
                    } else if (m.getType() == MessageType.DATA_SHORT){
                        Header header = Header.parse(m.getData());
                        if (header instanceof HeartbeatHeader) {
                            HeartbeatHeader heartbeatHeader = (HeartbeatHeader) header;

                            int sourceId = heartbeatHeader.getSourceId();
                            int nonce = heartbeatHeader.getNonce();
                            boolean isCollision = heartbeatHeader.isCollision();
                            int bitmap = heartbeatHeader.getBitmap();
                            boolean amNewcomer = isNewcomer(parameters.get("bitmap"));

                            System.out.println("SourceId " + parameters.get("sourceId"));
                            System.out.println("Our Bitmap.    : " + Integer.toBinaryString(parameters.get("bitmap")));
                            System.out.println("Incoming Bitmap: " + Integer.toBinaryString(bitmap));
                            System.out.println("Nonce " + parameters.get("nonce"));

                            // TODO: Check if to destroy
                            // TODO: Check logic again
                            // Collision between our ID and incoming ID - this can only happen when the nodes are in range of each other.
                            if ((sourceId == parameters.get("sourceId"))) {
                                // Current node is a newcomer. Newcomers always yield their ID and select a new ID from the available IDs.
                                // 1. Select random ID from IDs not used in the incoming bitmap.
                                // 2. Add these IDs to knownIDs (= routing table), associated with the incoming ID.
                                // 3. Take over the newly created bitmap.
                                // We did not force the other side to yield so from our point of view their ID is valid.
                                // 4. Update last heard heartbeat from the incoming ID.
                                // 5. Add the incoming ID as nextHop to themselves.
                                // 6. Send out updated bitmap including our bit.
                                if (amNewcomer) {
                                    randomId(bitmap, parameters, true);
                                    parameters.put("bitmap", updateKnownIds(0, bitmap, parameters.get("sourceId"), sourceId));
                                    updateHeartbeat(sourceId);
                                    knownIds.put(sourceId, sourceId);
                                    sendHeartbeat(heartbeatHeader, false);
                                // Current node is an established node.
                                } else {
                                    // Established nodes force newcomers to yield if there is a collision between IDs.
                                    // We don't incorporate their ID/bitmap and instead simply send them our heartbeat immediately - they will then yield and incorporate our bitmap.
                                    // This is also the case if our nonce is greater - we then know we have the right to this ID.
                                    if (isNewcomer(bitmap) || parameters.get("nonce") > nonce) {
                                        sendHeartbeat(heartbeatHeader, true);
                                    // If both nodes are established, we resolve based on the nonce.
                                    // If our nonces are equal, we know that both nodes should yield, so we can again generate the same ID
                                    // This is a fix incase exactly 1 ID is left. If we would disallow selection from the old ID,
                                    // may have selected the only ID left (i.e. both yielded), sent the packet, into another ID collision
                                    // or packet is simply lost due to packet collision (packet sent at same time). This is not a big problem
                                    // as on the next heartbeat, they would again attempt sync and the nonces are likely different
                                    // (or not and it would start over), but this likely allows for slightly faster convergence in general
                                    } else if (parameters.get("nonce") == nonce) {
                                        int finalBitmap = updateKnownIds(parameters.get("bitmap"), 0, parameters.get("sourceId"), sourceId);
                                        randomId(finalBitmap, parameters, false);
                                        sendHeartbeat(heartbeatHeader, false);
                                    } else {
                                        // We know that only we yield so from our point of view, their ID is valid.
                                        // We select a new random ID from the available IDs.
                                        // We incorporate their bitmap into ours.
                                        // We update last heard heartbeat from the incoming ID.
                                        // We add the incoming ID as nextHop to themselves.
                                        int initialBitmap = parameters.get("bitmap");
                                        randomId(bitmap, parameters, false);
                                        int finalBitmap = updateKnownIds(initialBitmap, parameters.get("bitmap"), parameters.get("sourceId"), sourceId);
                                        updateHeartbeat(sourceId);
                                        knownIds.put(sourceId, sourceId);
                                        sendHeartbeat(heartbeatHeader, false);
                                    }
                                }
                            } else {
                                if (amNewcomer) {
                                    if (!isNewcomer(bitmap) && isCollision) {
                                        System.out.println("BULLSHIT");
                                        int finalBitmap = updateKnownIds(0, bitmap, parameters.get("sourceId"), sourceId);
                                        randomId(finalBitmap, parameters, true);
                                        knownIds.put(sourceId, sourceId);
                                        sendHeartbeat(heartbeatHeader, false);
                                    } else {
                                        System.out.println("LSLSLSLS");
                                        parameters.put("bitmap", updateKnownIds(parameters.get("bitmap"), bitmap, parameters.get("sourceId"), sourceId));
                                        knownIds.put(sourceId, sourceId);
                                        sendHeartbeat(heartbeatHeader, false);
                                    }
                                    updateHeartbeat(sourceId);
                                } else if (bitmap != parameters.get("bitmap")) {
                                    if (isNewcomer(bitmap)) {
                                        if (!knownIds.containsKey(sourceId) && !lockedIds.contains(sourceId)) {
                                            System.out.println("DIFFERENT BITMAP");
                                            int mask = 1 << (sourceId - 1);
                                            System.out.println("sourceId: " + Integer.toBinaryString(sourceId));
                                            System.out.println("Mask: " + Integer.toBinaryString(mask));
                                            int newBitmap = (parameters.get("bitmap") & ~mask) | (bitmap & mask);
                                            LockThread lockThread = new LockThread();
                                            lockThread.boot(sourceId);
                                            lockThread.start();
                                            parameters.put("bitmap", updateKnownIds(parameters.get("bitmap"), newBitmap, parameters.get("sourceId"), sourceId));
                                            updateHeartbeat(sourceId);
                                            sendHeartbeat(heartbeatHeader, false);
                                        } else {
                                            System.out.println("ELSE");
                                            updateHeartbeat(sourceId);
                                            sendHeartbeat(heartbeatHeader, true);
                                        }
                                    } else {
                                        System.out.println("KKKKKK");
                                        parameters.put("bitmap", updateKnownIds(parameters.get("bitmap"), bitmap, parameters.get("sourceId"), sourceId));
                                        updateHeartbeat(sourceId);
                                        knownIds.put(sourceId, sourceId);
                                        sendHeartbeat(heartbeatHeader, false);
                                    }
                                // BITMAPS SAME - NO COLLISIONS
                                } else {
                                    knownIds.put(sourceId, sourceId);
                                    updateHeartbeat(sourceId);
                                }
                            }
                        } else if (header instanceof TokenHeader) {
                            TokenHeader tokenHeader = (TokenHeader) header;
                            if (tokenHeader.getNextHop() == parameters.get("sourceId") && tokenHeader.getDestId() != parameters.get("sourceId")){
                                if (knownIds.getOrDefault(tokenHeader.getDestId(), -1) != -1){
                                    tokenHeader.setNextHop(knownIds.get(tokenHeader.getDestId()));
                                    sendShort(tokenHeader);
                                }
                            } else if (tokenHeader.getNextHop() == parameters.get("sourceId") && tokenHeader.getDestId() == parameters.get("sourceId")) {
                                if (tokenHeader.getNonce() == 0xF) {
                                    if (knownIds.getOrDefault(tokenHeader.getSourceId(), -1) != -1) {
                                        Header ackTokenHeader = new TokenHeader(parameters.get("sourceId"), 0xE, tokenHeader.getSourceId(), knownIds.get(tokenHeader.getSourceId()));
                                        sendShort(ackTokenHeader);
                                        hasToken = true;
                                    }
                                } else if (tokenHeader.getNonce() == 0xE) {
                                    tokenAckReceived = true;
                                    hasToken = false;
                                }
                            }

                        } else if (header instanceof PayloadHeader) {
                            PayloadHeader ackHeader = (PayloadHeader) header;

                            // If we receive the ACK and w are the destination we should cancel the
                            // timeout and set the isSending as false.
                            //
                            // If we are not the destination id we should check if we are the
                            // nextHop in order to send to the next hop
                            if (ackHeader.isAck() && ackHeader.getDestId() == parameters.get("sourceId")){
                                ackReceived = true;

                                String key = createTimeoutKey(header, true);
                                Utils.Timeout.cancel(key);
                                isSending = false;
                            } else if (ackHeader.isAck()) {
                                int destId = ((PayloadHeader) header).getDestId();
                                int nextHopField = ((PayloadHeader) header).getNextHop();

                                // If we are the nextHop we should forward the message to the
                                //  next node in our routing table
                                if (nextHopField == parameters.get("destId")) {
                                    int nextHop = knownIds.getOrDefault(destId, -1);
                                    // forward the packet to the nextHop
                                    if (nextHop != -1) {
                                        // Copy m
                                        Message newAck = new Message(m.getType(), Utils.byteBufferClone(m.getData()));

                                        // Change header
                                        ((PayloadHeader)header).setNextHop(nextHop);

                                        sendingQueue.put(newAck);
                                    }
                                }
                            }
                        }
                        System.out.print("DATA_SHORT: ");
                        printByteBuffer(m.getData(), m.getData().capacity()); //Just print the data
                        printByteString(m.getData(), m.getData().capacity());

                    } else if (m.getType() == MessageType.DONE_SENDING){
                        System.out.println("DONE_SENDING");
                    } else if (m.getType() == MessageType.HELLO){
                        System.out.println("HELLO");
                    } else if (m.getType() == MessageType.SENDING){
                        System.out.println("SENDING");
                    } else if (m.getType() == MessageType.END){
                        System.out.println("END");
                        System.exit(0);
                    } else if (m.getType() == MessageType.TOKEN_ACCEPTED){
                        System.out.println("Token Valid!");
                    } else if (m.getType() == MessageType.TOKEN_REJECTED){
                        System.out.println("Token Rejected!");
                    }
                } catch (InterruptedException e){
                    System.err.println("Failed to take from queue: "+e);
                }
            }
        }

        private void sendAck(Header header) throws InterruptedException {
            Header copy = header.copy();
            int tmp = header.getSourceId();
            header.setSourceId(((PayloadHeader) header).getDestId());
            ((PayloadHeader) header).setDestId(tmp);

            int sourceId = header.getSourceId();

            int nextHop = knownIds.getOrDefault(sourceId, -1);
            // send ACK if there is a nextHop
            if (nextHop != -1) {
                ((PayloadHeader) header).setNextHop(nextHop);
                int assembledHeader = copy.assemble();

                ByteBuffer toSend = ByteBuffer.allocate(2);
                Message ackMsg = new Message(MessageType.DATA_SHORT, toSend);
                toSend.putShort((short) assembledHeader);

                sendingQueue.put(ackMsg);
            }
            // if the nextHop can't be found we ignore
        }
    }
}
