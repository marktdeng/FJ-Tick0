package uk.ac.cam.mtd36.fjava.tick0;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.BitSet;
import java.util.concurrent.Future;

import static java.lang.Math.toIntExact;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.nio.file.StandardOpenOption.*;

public class ExternalSort {

    private static void merge(String f1, String f2, int BufferSize) throws IOException {
        long blocksize = BufferSize;
        System.out.println("BLOCKSIZE = " + blocksize);
        Path wpath = Paths.get(f2);
        ByteBuffer wbuf = ByteBuffer.allocate(BufferSize);
        ByteBuffer abuf;

        AsynchronousFileChannel wafc = AsynchronousFileChannel.open(wpath, WRITE, READ, CREATE);
        Future wresult = null;
        long wloc = 0;
        long written = 0;

        FileSection fs1, fs2;
        long filesize = wafc.size();
        boolean popfirst = false;
        boolean swapfiles = true;
        while (blocksize < filesize){
            abuf = ByteBuffer.allocate(BufferSize);

            swapfiles = !swapfiles;
            String ftmp = f1;
            f1 = f2;
            f2 = ftmp;

            try {
                fs1 = new FileSection(BufferSize, blocksize, false, f1);
                fs2 = new FileSection(BufferSize, blocksize, true, f1);
            } catch (EOFException e) {
                e.printStackTrace();
                return;
            }

            try {
                while (true) {
                    if (fs1.peek() < fs2.peek()) {
                        popfirst = true;
                        abuf.asIntBuffer().put(fs1.pop());
                    } else {
                        popfirst = false;
                        abuf.asIntBuffer().put(fs2.pop());
                    }

                    if (written % BufferSize == 0) {
                        if (written > 0) {
                            while (wresult != null && !wresult.isDone()) {
                                //Waiting
                            }
                            wbuf.clear();
                        }
                        abuf.flip();

                        if (written + BufferSize >= filesize){
                            wbuf = abuf;
                            abuf = ByteBuffer.allocate(toIntExact(filesize - written));
                        } else {
                            ByteBuffer tmp;
                            tmp = wbuf;
                            wbuf = abuf;
                            abuf = tmp;
                        }

                        wresult = wafc.write(wbuf, wloc);

                        wloc += BufferSize;
                    }
                    written += 1;
                }
            } catch (EOFException e){
                //Do nothing - Should probably not use exceptions for control flow
            }

            FileSection fs;
            if (popfirst){
                fs = fs2;
            } else {
                fs = fs1;
            }

            try {
                while (true){
                    abuf.asIntBuffer().put(fs.pop());

                    if (written % BufferSize == 0) {
                        while (wresult != null && !wresult.isDone()) {
                            //Waiting
                        }
                        wbuf.clear();

                        abuf.flip();

                        if (written + BufferSize >= filesize){
                            wbuf = abuf;
                            abuf = ByteBuffer.allocate(toIntExact(filesize - written));
                        } else {
                            ByteBuffer tmp;
                            tmp = wbuf;
                            wbuf = abuf;
                            abuf = tmp;
                        }

                        wresult = wafc.write(wbuf, wloc);

                        wloc += BufferSize;
                    }
                    written += 1;
                }
            } catch (EOFException e){
                //Do nothing - Should probably not use exceptions for control flow
            }

            while (wresult != null && !wresult.isDone()) {
                //Waiting
            }

            blocksize *= 2;
        }
        wafc.close();
        if (swapfiles){
            java.nio.file.Files.copy(Paths.get(f2), Paths.get(f1), REPLACE_EXISTING);
        }
    }

    public static int[] read(String f1, long location, int size) throws IOException, EOFException {
        if (location + size > getsize(f1)) {
            size = (int) (getsize(f1) - location);
        }
        if (size <= 0) {
            throw new EOFException();
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
        try {
            int[] a = read(f1, 0, 2 * BufferSize);
            Arrays.sort(a);
            write(f1, a);
        } catch (EOFException e) {
            e.printStackTrace();
        }
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
            BufferSize = toIntExact(Long.highestOneBit(maxmemory - 1) / 32);
        } catch (ArithmeticException e){
            BufferSize = 1073741824;
        }

        //BufferSize = 65536;

        System.out.println("MEMORY = " + BufferSize);

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

        for (int i=0; i<filesize/BufferSize; i++){
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
            wbuf.asIntBuffer().put(st);
            wbuf.flip();
            wfuture = wafc.write(wbuf, location);
            rbuf.clear();
            location += tmpbufsize;
            System.out.println(location);
            if (location > filesize){
                throw new IllegalStateException();
            } else if (location + tmpbufsize >= filesize){
                tmpbufsize = toIntExact(filesize - location);
                if (tmpbufsize <= 0) {
                    break;
                }
                rbuf = ByteBuffer.allocate(tmpbufsize);
            }
            rfuture = rafc.read(rbuf, location);


        }

        rafc.close();
        wafc.close();

        merge(f2, f1, BufferSize);

        System.out.println(checkSum(f1));
    }


    public static void printfile(String f1, int BufferSize) throws IOException, EOFException {
        for (int i = 0; i < getsize(f1); i+= BufferSize){
            System.out.println(Arrays.toString(read(f1, i, BufferSize)).replace("[","").replace("]","").replace(",","\n").replace(" ", ""));
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