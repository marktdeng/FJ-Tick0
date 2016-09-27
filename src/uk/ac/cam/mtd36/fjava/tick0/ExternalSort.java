package uk.ac.cam.mtd36.fjava.tick0;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.Future;

import static java.lang.Math.toIntExact;
import static java.nio.file.StandardOpenOption.*;

public class ExternalSort {

    private static void merge(String f1, String f2, int bufSize) throws IOException {
        int blockSize = bufSize;

        long fileSize = getsize(f1);

        System.out.println("FILESIZE: " + fileSize);

        boolean swapFiles = true;

        while (blockSize < fileSize) {
            System.out.println("LOOP");
            String tmp = f1;
            f1 = f2;
            f2 = tmp;

            System.out.println(f1 + ">" + f2);

            swapFiles = !swapFiles;

            DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new RandomAccessFile(f2, "rw").getFD()), bufSize));

            FileBlock b1 = new FileBlock(f1, bufSize, blockSize, 0);
            FileBlock b2 = new FileBlock(f1, bufSize, blockSize, blockSize);

            mergeBlocks(writer, b1, b2);

            writer.flush();
            blockSize *= 2;
        }

        if (swapFiles) {
            FileBlock b = new FileBlock(f2, bufSize, fileSize, 0);
            DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new RandomAccessFile(f2, "rw").getFD()), bufSize));
            while (b.hasRemaining()) {
                writer.writeInt(b.pop());
            }
            writer.flush();
        }
    }

    private static void mergeBlocks(DataOutputStream writer, FileBlock b1, FileBlock b2) throws IOException {
        boolean continueLoop = true;
        while (continueLoop) {
            while (b1.hasRemaining() && b2.hasRemaining()) {
                if (b1.peek() > b2.peek()) {
                    writer.writeInt(b1.pop());
                } else {
                    writer.writeInt(b2.pop());
                }
            }
            FileBlock b;
            if (b1.hasRemaining()) {
                b = b1;
            } else {
                b = b2;
            }
            while (b.hasRemaining()) {
                writer.writeInt(b.pop());
            }

            boolean continue1, continue2;
            continue1 = b1.nextBlock();
            continue2 = b2.nextBlock();
            if (!continue1 && !continue2){
                throw new NotImplementedException();
            } else if (!continue1){
                throw new NotImplementedException();
            } else if (!continue2){
                continueLoop = false;
                copyBlock(writer, b1);
            }
        }
    }

    private static void copyBlock(DataOutputStream writer, FileBlock b) throws IOException {
        while (b.hasRemaining()) {
            writer.writeInt(b.pop());
        }
    }

    public static int[] read(String f1, long location, int size) throws IOException {
        if (location + size > getsize(f1)) {
            size = (int) (getsize(f1) - location);
        }
        Path path = Paths.get(f1);
        AsynchronousFileChannel afc = AsynchronousFileChannel.open(path, READ);
        ByteBuffer Buffer = ByteBuffer.allocate(size);
        Future result = afc.read(Buffer, location);
        while (!result.isDone()) {

        }
        Buffer.flip();
        int[] a = new int[size / 4];
        Buffer.asIntBuffer().get(a);
        afc.close();
        return a;
    }

    public static void write(String f1, int[] a) throws IOException {
        Path wpath = Paths.get(f1);
        AsynchronousFileChannel afc = AsynchronousFileChannel.open(wpath, WRITE, READ, CREATE);
        ByteBuffer Buffer = ByteBuffer.allocate(a.length * 4);
        Buffer.asIntBuffer().put(a);
        Future result = afc.write(Buffer, 0);
        while (!result.isDone()) {
            //Waiting
        }
        afc.close();
    }

    public static void basicsort(String f1, int BufferSize) throws IOException {
        int[] a = read(f1, 0, 64 * BufferSize);
        Arrays.sort(a);
        write(f1, a);
    }

    public static long getsize(String f1) throws IOException {
        File f = new File(f1);
        return f.length();
    }

    public static void sort(String f1, String f2) throws IOException {
        Runtime runtime = Runtime.getRuntime();
        long maxmemory = runtime.maxMemory();

        int BufferSize;
        try {
            BufferSize = toIntExact(Long.highestOneBit(maxmemory - 1) / 64);
        } catch (ArithmeticException e) {
            BufferSize = 1073741824;
        }
        //BufferSize = 65536;

        System.out.println("BUFSIZE = " + BufferSize);

        if (getsize(f1) < 16 * BufferSize) {
            System.out.println("BASIC SORT");
            basicsort(f1, BufferSize);
            System.out.println(checkSum(f1));
            return;
        }

        int[] st;
        long filesize = getsize(f1);

        int tmpbufsize = BufferSize;

        DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new RandomAccessFile(f2, "rw").getFD()), BufferSize));

        for (int i = 0; i < filesize; i += BufferSize) {
            if (i + tmpbufsize > filesize) {
                tmpbufsize = toIntExact(filesize - i);
            }
            FileBlock block = new FileBlock(f1, BufferSize, tmpbufsize, i);
            st = block.popall();
            Arrays.sort(st);
            for (int j : st) {
                writer.writeInt(j);
                //System.out.println(j);
            }
        }

        writer.flush();


        merge(f1, f2, BufferSize);
        printfile(f1, BufferSize);
        System.out.println(checkSum(f1));
        System.out.println(checkSum(f2));
    }


    public static void printfile(String f1, int BufferSize) throws IOException {
        FileWriter writer = new FileWriter("file.log");

        for (int i = 0; i < getsize(f1); i += BufferSize) {
            for (int j : read(f1, i, BufferSize)) {
                writer.write(Integer.toString(j) + "\n");
            }
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
            for (byte v : md.digest())
                computed += byteToHex(v);

            return computed;
        } catch (NoSuchAlgorithmException | IOException e) {
            e.printStackTrace();
        }
        return "<error computing checksum>";
    }

    public static void main(String[] args) throws Exception {
        String f1 = args[0];
        String f2 = args[1];
        sort(f1, f2);
        System.out.println("The checksum is: " + checkSum(f1));
    }


}