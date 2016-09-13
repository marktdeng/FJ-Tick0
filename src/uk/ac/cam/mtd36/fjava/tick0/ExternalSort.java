package uk.ac.cam.mtd36.fjava.tick0;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.concurrent.Future;

import static java.lang.Math.toIntExact;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;

public class ExternalSort {

    private static void merge(String f1, String f2, int bufSize) throws IOException {
        long blockSize = bufSize;

        long fileSize = getsize(f1);

        boolean swapFiles = true;

        while (blockSize * 2 < fileSize) {
            String tmp = f1;
            f1 = f2;
            f2 = tmp;

            swapFiles = !swapFiles;

            FileWriter writer = new FileWriter(f2, bufSize, fileSize);
            long loc;
            for (loc = 0; loc < fileSize; loc += blockSize * 2){
                mergeBlocks(f1, writer, bufSize, blockSize, blockSize, loc);
            }
            loc -= blockSize * 2;
            if (fileSize - loc > blockSize){
                long bSize = fileSize - loc - blockSize;
                mergeBlocks(f1, writer, bufSize, blockSize, bSize, loc);
            } else {
                long bSize = fileSize - loc;
                System.out.println("FILESIZE: " + fileSize + "LOC: " + loc);
                copyBlock(f1, writer, bufSize, bSize, loc);
            }

            blockSize *= 2;
        }

        if (swapFiles){
            Files.copy(Paths.get(f2), Paths.get(f1), REPLACE_EXISTING);
        }
    }
    
    private static void mergeBlocks(String file, FileWriter writer, int bufSize,
                                    long blockSize1, long blockSize2, long loc) throws IOException{
        FileBlock b1 = new FileBlock(file, bufSize, blockSize1, loc);
        FileBlock b2 = new FileBlock(file, bufSize, blockSize2, loc + blockSize1);
        while (!b1.hasRemaining() && b2.hasRemaining()){
            if (b1.peek() > b2.peek()){
                writer.add(b1.pop());
            } else {
                writer.add(b2.pop());
            }
        }
        FileBlock b;
        if (b1.hasRemaining()){
            b = b1;
        } else {
            b = b2;
        }
        while (b.hasRemaining()){
            writer.add(b.pop());
        }
    }

    private static void copyBlock(String file, FileWriter writer, int bufSize, long blocksize, long loc) throws IOException {
        System.out.println("COPY");
        FileBlock b = new FileBlock(file, bufSize, blocksize, loc);
        if (b.hasRemaining()) {
            while (b.hasRemaining()) {
                writer.add(b.pop());
            }
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

        int[] a = read(f1, 0, 2 * BufferSize);
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

        if (getsize(f1) < 2 * BufferSize) {
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
            System.out.println(location + ":" + Arrays.toString(read(f1, i, BufferSize)).replace("[", "").replace("]", "").replace(",", "\n" + location++ + ":").replace(" ", ""));
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