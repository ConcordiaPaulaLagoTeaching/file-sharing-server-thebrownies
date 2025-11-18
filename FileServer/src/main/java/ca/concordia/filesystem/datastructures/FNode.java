package ca.concordia.filesystem.datastructures;

import java.io.Serializable;

public class FNode implements Serializable {

    private static final long serialVersionUID = 1L;

    private short blockIndex;
    private short nextIndex;

    public FNode() {
        this.blockIndex = -1;
        this.nextIndex = -1;
    }

    public boolean isUsed() {
        return blockIndex >= 0;
    }

    public short getBlockIndex() {
        return blockIndex;
    }

    public void setBlockIndex(short blockIndex) {
        this.blockIndex = blockIndex;
    }

    public short getNext() {
        return nextIndex;
    }

    public void setNext(short nextIndex) {
        this.nextIndex = nextIndex;
    }

    public void clear() {
        this.blockIndex = -1;
        this.nextIndex = -1;
    }

    @Override
    public String toString() {
        return "[FNode block=" + blockIndex + " next=" + nextIndex + "]";
    }
}
