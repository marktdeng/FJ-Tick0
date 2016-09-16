package uk.ac.cam.mtd36.fjava.tick0;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
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
        long blockSize = bufSize;

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
            long loc;
            for (loc = 0; loc + blockSize*2 < fileSize; loc += blockSize * 2){
                mergeBlocks(f1, writer, bufSize, blockSize, blockSize, loc);
            }
            System.out.println("LOCATION: " + loc);
            if (fileSize - loc > blockSize){
                long bSize = fileSize - loc - blockSize;
                mergeBlocks(f1, writer, bufSize, blockSize, bSize, loc);
            } else {
                long bSize = fileSize - loc;
                copyBlock(f1, writer, bufSize, bSize, loc);
            }
            blockSize *= 2;
            writer.flush();
        }

        if (swapFiles){
            FileBlock b = new FileBlock(f2, bufSize, fileSize, 0, 1);
            DataOutputStream writer = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(new RandomAccessFile(f2, "rw").getFD()), bufSize));
            while (b.hasRemaining()){
                writer.writeInt(b.pop());
            }
            writer.flush();
        }
    }

    private static void mergeBlocks(String file, DataOutputStream writer, int bufSize,
                                    long blockSize1, long blockSize2, long loc) throws IOException{
        try {
            System.out.println("MERGE B1: " + blockSize1 + " B2: " + blockSize2 + " LOC: " + loc);
            FileBlock b1 = new FileBlock(file, bufSize, blockSize1, loc, 1);
            FileBlock b2 = new FileBlock(file, bufSize, blockSize2, loc + blockSize1, 2);
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
        } catch (NotImplementedException e){
            e.printStackTrace();
            printfile(file, bufSize);
        }
    }

    private static void copyBlock(String file, DataOutputStream writer, int bufSize, long blockSize, long loc) throws IOException {
        System.out.println("COPY B: " + blockSize + " LOC: " + loc);
        FileBlock b = new FileBlock(file, bufSize, blockSize, loc, 1);
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
            BufferSize = toIntExact(Long.highestOneBit(maxmemory - 1) / 128);
        } catch (ArithmeticException e) {
            BufferSize = 1073741824;
        }

        System.out.println("BUFSIZE = " + BufferSize);

        if (getsize(f1) < 32 * BufferSize) {
            System.out.println("BASIC SORT");
            basicsort(f1, BufferSize);
            System.out.println(checkSum(f1));
            return;
        }

        Path rpath = Paths.get(f1);
        Path wpath = Paths.get(f2);

        AsynchronousFileChannel rafc = AsynchronousFileChannel.open(rpath, READ);
        AsynchronousFileChannel wafc = AsynchronousFileChannel.open(wpath, WRITE, READ, CREATE);

        ByteBuffer rbuf = ByteBuffer.allocate(BufferSize);
        ByteBuffer wbuf = ByteBuffer.allocate(BufferSize);

        Future wfuture = null;
        Future rfuture = rafc.read(rbuf, 0);

        int[] st;
        int location = 0;
        long filesize = getsize(f1);

        int tmpbufsize = BufferSize;

        for (int i = 0; i <= filesize / BufferSize; i++) {
            while (!rfuture.isDone()) {
                //Waiting
            }
            rbuf.flip();
            st = new int[tmpbufsize / 4];
            rbuf.asIntBuffer().get(st);
            Arrays.sort(st);
            if (i != 0) {
                while (!wfuture.isDone()) {
                    //Waiting
                }
            }
            wbuf.clear();
            if (tmpbufsize != BufferSize) {
                wbuf = ByteBuffer.allocate(tmpbufsize);
                wbuf.clear();
            }
            wbuf.asIntBuffer().put(st);
            wfuture = wafc.write(wbuf, location);
            rbuf.clear();
            location += tmpbufsize;
            if (location > filesize) {
                throw new IllegalStateException();
            } else if (location + tmpbufsize >= filesize) {
                tmpbufsize = toIntExact(filesize - location);
                if (tmpbufsize <= 0) {
                    break;
                }
                rbuf = ByteBuffer.allocate(tmpbufsize);
                rbuf.clear();
            }

            rfuture = rafc.read(rbuf, location);

        }

        rafc.close();
        wafc.close();


        merge(f1, f2, BufferSize);

        System.out.println(checkSum(f1));
    }


    public static void printfile(String f1, int BufferSize) throws IOException {
        long location = 0;

        for (int i = 0; i < getsize(f1); i += BufferSize) {
            for (int j:read(f1,i,BufferSize)) {
                System.out.println(location++ + ":" + j);
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