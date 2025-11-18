package ca.concordia.filesystem;

import ca.concordia.filesystem.datastructures.FEntry;
import ca.concordia.filesystem.datastructures.FNode;
import java.io.*;
import java.util.*;

import java.util.concurrent.locks.*;

public class FileSystemManager {

    private static final int BLOCK_SIZE = 128;
    private static final int MAX_FILE_ENTRIES = 16;
    private static final int MAX_BLOCKS = 64;

    private final FEntry[] fileEntries = new FEntry[MAX_FILE_ENTRIES];
    private final FNode[] fileNodes = new FNode[MAX_BLOCKS];

    private final ReadWriteLock fileSystemLock = new ReentrantReadWriteLock(true);

    private RandomAccessFile disk;
    private int dataStartBlock;
    private int metadataSize;
    private int metadataBlockCount;

    public FileSystemManager(String diskFilePath, int totalSize) throws Exception {

        for (int i = 0; i < MAX_FILE_ENTRIES; i++) fileEntries[i] = new FEntry();
        for (int i = 0; i < MAX_BLOCKS; i++) fileNodes[i] = new FNode();

        int fileEntrySize = 15;
        int fileNodeSize = 4;
        metadataSize = MAX_FILE_ENTRIES * fileEntrySize + MAX_BLOCKS * fileNodeSize;
        metadataBlockCount = (int) Math.ceil((double) metadataSize / BLOCK_SIZE);
        dataStartBlock = metadataBlockCount;

        disk = new RandomAccessFile(diskFilePath, "rw");

        if (disk.length() == 0) {
            disk.setLength(totalSize);
            saveMetadata();
        } else {
            loadMetadata();
        }
    }

    // Metadata I/O

    private void saveMetadata() throws IOException {
        fileSystemLock.writeLock().lock();
        try {
            disk.seek(0);
            ByteArrayOutputStream byteOutStream = new ByteArrayOutputStream();

            for (FEntry entry : fileEntries) writeFileEntry(byteOutStream, entry);
            for (FNode node : fileNodes) writeFileNode(byteOutStream, node);

            byte[] metadataBytes = byteOutStream.toByteArray();
            disk.write(metadataBytes, 0, metadataBytes.length);

            int padding = (metadataBlockCount * BLOCK_SIZE) - metadataBytes.length;
            if (padding > 0) disk.write(new byte[padding]);

        } finally {
            fileSystemLock.writeLock().unlock();
        }
    }

    private void loadMetadata() throws IOException {
        fileSystemLock.writeLock().lock();
        try {
            disk.seek(0);
            byte[] metadataBytes = new byte[metadataBlockCount * BLOCK_SIZE];
            disk.readFully(metadataBytes);

            ByteArrayInputStream byteInStream = new ByteArrayInputStream(metadataBytes);

            for (int i = 0; i < MAX_FILE_ENTRIES; i++) fileEntries[i] = readFileEntry(byteInStream);
            for (int i = 0; i < MAX_BLOCKS; i++) fileNodes[i] = readFileNode(byteInStream);

        } finally {
            fileSystemLock.writeLock().unlock();
        }
    }

    // Metadata Serialization Helpers

    private void writeFileEntry(OutputStream out, FEntry entry) throws IOException {
        byte[] name = new byte[11];
        if (entry.getFilename() != null)
            System.arraycopy(entry.getFilename().getBytes(), 0, name, 0, entry.getFilename().length());

        out.write(name);
        writeShortToStream(out, entry.getFilesize());
        writeShortToStream(out, entry.getFirstBlock());
    }

    private FEntry readFileEntry(InputStream in) throws IOException {
        byte[] name = in.readNBytes(11);
        short size = readShortFromStream(in);
        short firstBlock = readShortFromStream(in);

        FEntry entry = new FEntry();
        String entryName = new String(name).trim();
        if (!entryName.isEmpty()) entry.setFilename(entryName);
        entry.setFilesize(size);
        entry.setFirstBlock(firstBlock);
        return entry;
    }

    private void writeFileNode(OutputStream out, FNode node) throws IOException {
        writeShortToStream(out, node.getBlockIndex());
        writeShortToStream(out, node.getNext());
    }

    private FNode readFileNode(InputStream in) throws IOException {
        short blockIndex = readShortFromStream(in);
        short next = readShortFromStream(in);
        FNode node = new FNode();
        node.setBlockIndex(blockIndex);
        node.setNext(next);
        return node;
    }

    private void writeShortToStream(OutputStream out, short value) throws IOException {
        out.write((value >> 8) & 0xFF);
        out.write(value & 0xFF);
    }

    private short readShortFromStream(InputStream in) throws IOException {
        int highByte = in.read();
        int lowByte = in.read();
        return (short) ((highByte << 8) | lowByte);
    }

    // File Operations

    public void createFile(String filename) throws Exception {
        fileSystemLock.writeLock().lock();
        try {
            if (filename == null || filename.length() > 11)
                throw new Exception("ERROR: filename too large");

            for (FEntry entry : fileEntries)
                if (entry.isUsed() && filename.equals(entry.getFilename()))
                    throw new Exception("ERROR: file already exists");

            int freeSlot = -1;
            for (int i = 0; i < MAX_FILE_ENTRIES; i++)
                if (!fileEntries[i].isUsed()) { freeSlot = i; break; }

            if (freeSlot == -1)
                throw new Exception("ERROR: maximum file limit reached");

            fileEntries[freeSlot] = new FEntry(filename);
            saveMetadata();

        } finally {
            fileSystemLock.writeLock().unlock();
        }
    }

    public byte[] readFile(String filename) throws Exception {
        fileSystemLock.readLock().lock();
        try {
            FEntry entry = findFileEntry(filename);
            if (entry == null)
                throw new Exception("ERROR: file " + filename + " does not exist");

            byte[] data = new byte[entry.getFilesize()];
            int offset = 0;
            short nodeIndex = entry.getFirstBlock();

            while (nodeIndex != -1) {
                FNode node = fileNodes[nodeIndex];
                int realBlockIndex = dataStartBlock + node.getBlockIndex();

                disk.seek(realBlockIndex * BLOCK_SIZE);

                int bytesToRead = Math.min(BLOCK_SIZE, data.length - offset);
                disk.readFully(data, offset, bytesToRead);

                offset += bytesToRead;
                nodeIndex = node.getNext();
            }

            return data;
        } finally {
            fileSystemLock.readLock().unlock();
        }
    }

    public void deleteFile(String filename) throws Exception {
        fileSystemLock.writeLock().lock();
        try {
            FEntry entry = findFileEntry(filename);
            if (entry == null)
                throw new Exception("ERROR: file " + filename + " does not exist");

            short nodeIndex = entry.getFirstBlock();
            byte[] zeros = new byte[BLOCK_SIZE];

            while (nodeIndex != -1) {
                FNode node = fileNodes[nodeIndex];

                int realBlockIndex = dataStartBlock + node.getBlockIndex();
                disk.seek(realBlockIndex * BLOCK_SIZE);
                disk.write(zeros);

                short nextNode = node.getNext();
                node.clear();
                nodeIndex = nextNode;
            }

            entry.clear();
            saveMetadata();

        } finally {
            fileSystemLock.writeLock().unlock();
        }
    }

    public void writeFile(String filename, byte[] data) throws Exception {
        fileSystemLock.writeLock().lock();
        try {
            FEntry entry = findFileEntry(filename);
            if (entry == null)
                throw new Exception("ERROR: file " + filename + " does not exist");

            int requiredBlocks = (int) Math.ceil(data.length / (double) BLOCK_SIZE);
            if (requiredBlocks == 0) requiredBlocks = 1;

            List<Integer> freeBlocks = new ArrayList<>();
            for (int i = 0; i < MAX_BLOCKS; i++)
                if (!fileNodes[i].isUsed()) freeBlocks.add(i);

            if (freeBlocks.size() < requiredBlocks)
                throw new Exception("ERROR: not enough free blocks");

            int[] allocatedBlocks = new int[requiredBlocks];
            for (int i = 0; i < requiredBlocks; i++) allocatedBlocks[i] = freeBlocks.get(i);

            for (int i = 0; i < requiredBlocks; i++) {
                int blockIndex = allocatedBlocks[i];
                fileNodes[blockIndex].setBlockIndex((short) blockIndex);
                fileNodes[blockIndex].setNext(i == requiredBlocks - 1 ? (short) -1 : (short) allocatedBlocks[i + 1]);
            }

            int offset = 0;
            for (int i = 0; i < requiredBlocks; i++) {
                int blockIndex = dataStartBlock + allocatedBlocks[i];
                disk.seek(blockIndex * BLOCK_SIZE);

                int bytesToWrite = Math.min(BLOCK_SIZE, data.length - offset);
                disk.write(data, offset, bytesToWrite);

                if (bytesToWrite < BLOCK_SIZE)
                    disk.write(new byte[BLOCK_SIZE - bytesToWrite]);

                offset += bytesToWrite;
            }

            short oldFirstBlock = entry.getFirstBlock();
            byte[] zeros = new byte[BLOCK_SIZE];
            while (oldFirstBlock != -1) {
                FNode node = fileNodes[oldFirstBlock];
                int blockIndex = dataStartBlock + node.getBlockIndex();
                disk.seek(blockIndex * BLOCK_SIZE);
                disk.write(zeros);

                short nextBlock = node.getNext();
                node.clear();
                oldFirstBlock = nextBlock;
            }

            entry.setFilesize((short) data.length);
            entry.setFirstBlock((short) allocatedBlocks[0]);

            saveMetadata();

        } finally {
            fileSystemLock.writeLock().unlock();
        }
    }

    public String[] listFiles() {
        fileSystemLock.readLock().lock();
        try {
            List<String> fileNames = new ArrayList<>();
            for (FEntry entry : fileEntries)
                if (entry.isUsed()) fileNames.add(entry.getFilename());
            return fileNames.toArray(new String[0]);
        } finally {
            fileSystemLock.readLock().unlock();
        }
    }

    private FEntry findFileEntry(String filename) {
        for (FEntry entry : fileEntries)
            if (entry.isUsed() && filename.equals(entry.getFilename()))
                return entry;
        return null;
    }
}
