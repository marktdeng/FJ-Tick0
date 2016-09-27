package uk.ac.cam.mtd36.fjava.tick0;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.util.ArrayList;

public class FileBlock {
    private DataInputStream input;
    private long blockSize;
    private boolean popped;
    private int previous = -2147483648;
    private int lastRead;
    private long startLoc;
    private int count = 0;
    private boolean checkConsistency = true;

    public FileBlock(String file, int bufSize, long blockSize, int startLoc) throws IOException {
        this.blockSize = blockSize;
        this.startLoc = startLoc;
        this.input = new DataInputStream(new BufferedInputStream(new FileInputStream(new RandomAccessFile(file, "rw").getFD()), bufSize));
        int skipped = input.skipBytes(startLoc);
        if (startLoc != skipped){
            throw new IOException();
        }

        System.out.println("NEW BLOCK: " + this.blockSize + " START: " + startLoc + " SKIPPED: " + skipped);

        lastRead = input.readInt();
    }

    public int peek() throws IOException {
        if (popped){
            try {
                lastRead = input.readInt();
                popped = false;
                return lastRead;
            } catch (EOFException e) {
                e.printStackTrace();
                System.exit(1);
                return lastRead;
            }
        } else {
            return lastRead;
        }
    }

    public int pop() throws IOException {
        if (popped) {
            lastRead = input.readInt();
        }
        int result = lastRead;
        popped = true;
        count++;
        if (previous > result && checkConsistency){
            System.out.println("PREVIOUS: " + previous + " RESULT: " + result + " LOCATION: " + (count + startLoc));
            throw new NotImplementedException();
        } else {
            previous = result;
        }
        return result;
    }

    public int[] popall() throws IOException{
        checkConsistency = false;
        ArrayList<Integer> l = new ArrayList<>();
        try {
            while (hasRemaining()) {
                l.add(pop());
            }
        } catch (EOFException e){
            //Continue
        }
        return l.stream().mapToInt(i->i).toArray();
    }

    public boolean hasRemaining() throws IOException{
        return (count * 4 < blockSize);
    }

    public boolean nextBlock() throws IOException {
        long skipped = input.skip(blockSize);
        previous = -2147483648;
        count = 0;
        if (skipped == blockSize){
            peek();
            return true;
        } else {
            return false;
        }

    }
}
