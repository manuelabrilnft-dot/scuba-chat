public class PayloadHeader implements Header {
    private boolean isAck;
    private int sourceId;
    private int destId;
    private boolean isLastFragment;
    private int nextHop;
    private int sequenceNumber;

    public PayloadHeader(boolean isAck, int sourceId, int destId, boolean isLastFragment, int nextHop, int sequenceNumber) {
        this.isAck = isAck;
        this.sourceId = sourceId;
        this.destId = destId;
        this.isLastFragment = isLastFragment;
        this.nextHop = nextHop;
        this.sourceId = sourceId;
        this.sequenceNumber = sequenceNumber;
    }

    @Override
    public int assemble() {
        int output = 0;
        output = (sourceId << 12) | output;
        output = (destId << 9) | output;
        output = ((isAck ? 1 : 0) << 8) | output;
        output = ((isLastFragment ? 1 : 0) << 7) | output;
        output = (nextHop << 4) | output;
        output = (sequenceNumber) | output;
        return output;
    }

    @Override
    public boolean isAck() {
        return isAck;
    }

    public void isAck(boolean isAck) {
        this.isAck = isAck;
    }

    @Override
    public int getSourceId() {
        return sourceId;
    }

    @Override
    public void setSourceId(int sourceId) {
        this.sourceId = sourceId;
    }

    public int getDestId() {
        return destId;
    }

    public int getSequenceNumber() {
        return sequenceNumber;
    }

    public void setDestId(int destId) {
        this.destId = destId;
    }

    public boolean isLastFragment() {
        return isLastFragment;
    }

    public int getNextHop() {
        return nextHop;
    }

    public void setNextHop(int nextHop) {
        this.nextHop = nextHop;
    }

    public Header copy() {
        return new PayloadHeader(isAck, sourceId, destId, isLastFragment, nextHop, sequenceNumber);
    }

}
