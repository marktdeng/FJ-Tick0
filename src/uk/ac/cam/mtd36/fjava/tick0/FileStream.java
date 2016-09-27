package uk.ac.cam.mtd36.fjava.tick0;

import java.io.*;

public class FileStream {
    public static int staticBlockNum = 0;
    public int blockNum = staticBlockNum++;
    private DataInputStream input;
    private long blockSize;
    private int head;
    private int read;

    public FileStream(String fileName, int bufSize, long blockSize, long startLoc) throws IOException{
        //bufSize in bytes
        //blockSize, startLoc in number of ints



        RandomAccessFile file = new RandomAccessFile(fileName, "r");
        input = new DataInputStream(new BufferedInputStream(new FileInputStream(file.getFD()), bufSize));

        this.blockSize = blockSize;
        read = 0;

        long skipped = input.skip(startLoc * 4);
        assert skipped == startLoc*4;

        head = input.readInt();
        read++;
    }

    public int peek(){
        return head;
    }

    public int pop() throws IOException {
        int result = head;
        if (read < blockSize){
            head = input.readInt();
        }
        read++;
        return result;
    }

    public boolean ready() throws IOException {
        if (read <= blockSize){
            return true;
        } else {
            input.close();
            return false;
        }
    }
}
