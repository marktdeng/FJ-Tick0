package uk.ac.cam.mtd36.fjava.tick0;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import static java.lang.Math.toIntExact;

public class ExternalSort {
    private static int memSize; //in bytes
    private static int blockSize; //in number of ints
    private static long fileSize; //in number of ints


    public static void sort(String f1, String f2) throws IOException {
        //Get amount of memory available
        long maxMemory = Runtime.getRuntime().maxMemory();
        try {
            memSize = toIntExact(Long.highestOneBit(maxMemory - 1) / 16);
        } catch (ArithmeticException e) {
            memSize = 1073741824;
        }

        if (prepare(f1, f2)){
            merge(f1, f2);
        }

    }

    private static boolean prepare(String f1, String f2) throws IOException {
        RandomAccessFile readFile = new RandomAccessFile(f1, "r");

        blockSize = memSize;
        DataInputStream inStream = new DataInputStream(new BufferedInputStream(new FileInputStream(readFile.getFD()), blockSize));

        fileSize = readFile.length() / 4;

        if (fileSize <= blockSize){
            //Entire file fits in memory

            RandomAccessFile writeFile = new RandomAccessFile(f1, "rw");
            DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(writeFile.getFD()), blockSize));
            byte[] bArray = new byte[toIntExact(fileSize*4)];
            inStream.readFully(bArray); //Read whole file

            //Convert byte array to int array
            IntBuffer intBuf = ByteBuffer.wrap(bArray).asIntBuffer();
            int[] iArray = new int[toIntExact(fileSize)];
            intBuf.get(iArray);

            Arrays.sort(iArray);
            for (int i : iArray) {
                outStream.writeInt(i);
            }
            outStream.flush();
            return false;
        } else {
            //Need to use merge sort
            //Sort blocks of file with java default sort

            RandomAccessFile writeFile = new RandomAccessFile(f2, "rw");
            DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(writeFile.getFD()), blockSize));
            int loc = 0;
            int tmpBufSize = blockSize;
            if (fileSize <= tmpBufSize){
                tmpBufSize = toIntExact(fileSize * 4);
            }
            while (loc < fileSize){
                if (loc + tmpBufSize > fileSize){
                    tmpBufSize = toIntExact((fileSize - loc));
                }

                int[] iArray = new int[tmpBufSize];
                for (int i = 0; i< tmpBufSize; i++){
                    iArray[i] = inStream.readInt();
                }

                Arrays.sort(iArray);
                for (int i : iArray) {
                    loc++;
                    outStream.writeInt(i);
                }
                outStream.flush();
            }
            outStream.flush();
            return true;
        }
    }

    private static void merge(String f1, String f2) throws IOException {
        RandomAccessFile writeFile = new RandomAccessFile(f1, "rw");
        DataOutputStream outStream = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(writeFile.getFD()), memSize/8));

        int lastBlockSize = toIntExact(fileSize % blockSize);
        int numBlocks = toIntExact(fileSize/blockSize) + 1;
        int bufSize = toIntExact(Long.highestOneBit(memSize*4/(numBlocks)));
        if (bufSize > blockSize * 4){
            bufSize = toIntExact(blockSize * 4);
        }

        MinHeap blocks = new MinHeap(numBlocks);

        //Add all blocks to a heap
        for (int i = 0; i < numBlocks; i++){
            if (i == numBlocks - 1){
                blocks.add(new FileStream(f2, bufSize, lastBlockSize, i*blockSize));
            } else {
                blocks.add(new FileStream(f2, bufSize, blockSize, i * blockSize));
            }
        }

        //pop smallest item from heap and reinsert
        while (!blocks.isEmpty()){
            FileStream f = blocks.pop();
            int result = f.pop();
            outStream.writeInt(result);
            if (f.ready()){
                blocks.add(f);
            }
        }
        outStream.flush();
    }

    public static void printFile(String f) throws IOException{
        RandomAccessFile readFile = new RandomAccessFile(f, "r");
        int bufSize = memSize;
        DataInputStream inStream = new DataInputStream(new BufferedInputStream(new FileInputStream(readFile.getFD()), bufSize));
        for (int i = 0; i < fileSize; i++){
            System.out.println(inStream.readInt());
        }
    }

    private static String byteToHex(byte b) {
        String r = Integer.toHexString(b);
        if (r.length() == 8) {
            return r.substring(6);
        }
        return r;
    }

    public static String checkSum(String f) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            DigestInputStream ds = new DigestInputStream(
                    new FileInputStream(f), md);
            byte[] b = new byte[512];
            while (ds.read(b) != -1)
                ;

            String computed = "";
            for(byte v : md.digest())
                computed += byteToHex(v);

            return computed;
        } catch (NoSuchAlgorithmException|IOException e) {
            e.printStackTrace();
        }
        return "<error computing checksum>";
    }

    public static void main(String[] args) throws Exception {
        String f1 = args[0];
        String f2 = args[1];
        sort(f1, f2);
        System.out.println("The checksum is: "+checkSum(f1));
    }
}