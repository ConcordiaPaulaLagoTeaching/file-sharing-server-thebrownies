package ca.concordia.filesystem.datastructures;

import java.io.Serializable;

public class FEntry implements Serializable {

    private static final long serialVersionUID = 1L;

    private String filename;
    private short filesize;
    private short firstBlock;

    public FEntry() {
        this.filename = null;
        this.filesize = 0;
        this.firstBlock = -1;
    }

    public FEntry(String filename) {
        setFilename(filename);
        this.filesize = 0;
        this.firstBlock = -1;
    }

    public boolean isUsed() {
        return filename != null && !filename.isEmpty();
    }

    // Getters and Setters
    public String getFilename() {
        return filename;
    }

    public void setFilename(String filename) {
        if (filename == null || filename.length() > 11)
            throw new IllegalArgumentException("Filename cannot exceed 11 characters");
        this.filename = filename;
    }

    public short getFilesize() {
        return filesize;
    }

    public void setFilesize(short filesize) {
        this.filesize = filesize;
    }

    public short getFirstBlock() {
        return firstBlock;
    }

    public void setFirstBlock(short firstBlock) {
        this.firstBlock = firstBlock;
    }

    public void clear() {
        this.filename = null;
        this.filesize = 0;
        this.firstBlock = -1;
    }

    @Override
    public String toString() {
        return "[FEntry " + filename + " size=" + filesize + " first=" + firstBlock + "]";
    }
}
