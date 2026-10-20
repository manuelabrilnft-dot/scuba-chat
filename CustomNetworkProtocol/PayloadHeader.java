/**
 * Payload packet header (MSB = 0).
 *
 * Byte 0: [0 | src(3) | dst(3) | isAck(1)]
 * Byte 1: [isLast(1) | nextHop(3) | seq(4)]
 */
public class PayloadHeader implements Header {
    private boolean isAck;
    private int sourceId;
    private int destId;
    private boolean isLastFragment;
    private int nextHop;
    private int sequenceNumber;

    public PayloadHeader(boolean isAck, int sourceId, int destId,
                         boolean isLastFragment, int nextHop, int sequenceNumber) {
        this.isAck = isAck;
        this.sourceId = sourceId;
        this.destId = destId;
        this.isLastFragment = isLastFragment;
        this.nextHop = nextHop;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public int assemble() {
        int out = 0;
        out |= (sourceId & 0x07) << 12;
        out |= (destId   & 0x07) << 9;
        out |= (isAck ? 1 : 0) << 8;
        out |= (isLastFragment ? 1 : 0) << 7;
        out |= (nextHop  & 0x07) << 4;
        out |= (sequenceNumber & 0x0F);
        return out & 0xFFFF;
    }

    @Override
    public Header copy() {
        return new PayloadHeader(isAck, sourceId, destId, isLastFragment, nextHop, sequenceNumber);
    }

    @Override public boolean isAck() { return isAck; }
    public void setAck(boolean ack) { this.isAck = ack; }

    @Override public int getSourceId() { return sourceId; }
    @Override public void setSourceId(int sourceId) { this.sourceId = sourceId; }

    public int getDestId() { return destId; }
    public void setDestId(int destId) { this.destId = destId; }

    public boolean isLastFragment() { return isLastFragment; }
    public void setLastFragment(boolean last) { this.isLastFragment = last; }

    public int getNextHop() { return nextHop; }
    public void setNextHop(int nextHop) { this.nextHop = nextHop; }

    public int getSequenceNumber() { return sequenceNumber; }
    public void setSequenceNumber(int seq) { this.sequenceNumber = seq; }
}
