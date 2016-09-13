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

    private static void merge(String f1, String f2, int bufsize) throws IOException {
        long blocksize = bufsize;

        AsynchronousFileChannel wafc;

        Future wresult = null;
        long wloc = 0;
        long written = 0;
        boolean swapfiles = false;
        FileBlock[] blocks;

        FileSection fs1, fs2, fs;
        long filesize = getsize(f1);
        System.out.println("FILESIZE = " + filesize);



        while (blocksize < filesize) {

            System.out.println("BLOCKSIZE = " + blocksize);

            swapfiles = !swapfiles;
            String ftmp = f1;
            f1 = f2;
            f2 = ftmp;

            wafc = AsynchronousFileChannel.open(Paths.get(f1));

            try {
                blocks = FileBlock.getblocks(blocksize, bufsize, filesize, f1);
            } catch (EndBlockException e){
                e.printStackTrace();
                return;
            }

            for (int i = 0; i<blocks.length; i+=2){
                if (i + 1 < blocks.length){
                    mergeblocks(blocks[i], blocks[i+1], wafc, bufsize);
                    blocks[i] = null;
                    blocks[i+1] = null;
                } else {
                    copyblock(blocks[i], wafc, bufsize);
                    blocks[i] = null;
                }
            }

            //printfile(f2, bufsize);

            blocksize *= 2;
        }

        if (swapfiles) {
            java.nio.file.Files.copy(Paths.get(f2), Paths.get(f1), REPLACE_EXISTING);
        }
    }

    private static void copyblock(FileBlock a, AsynchronousFileChannel afc, int bufsize) throws IOException {
        Future wresult = null;
        long TotalBlockSize = a.getBlocksize();
        long written = 0;
        long wlocation = a.getStartlocation();
        if (bufsize > TotalBlockSize){
            bufsize = Math.toIntExact(TotalBlockSize);
        }

        ByteBuffer abuf = ByteBuffer.allocate(bufsize);
        ByteBuffer wbuf = ByteBuffer.allocate(bufsize);

        try {
            while (true) {
                abuf.asIntBuffer().put(a.pop());

                if (!abuf.hasRemaining()) {
                    while (wresult != null && !wresult.isDone()) {
                        //Waiting
                    }
                    wbuf.clear();
                    abuf.flip();

                    if (written + bufsize >= TotalBlockSize) {
                        wbuf = abuf;
                        abuf = ByteBuffer.allocate(toIntExact(TotalBlockSize - written));
                    } else {
                        ByteBuffer tmp;
                        tmp = wbuf;
                        wbuf = abuf;
                        abuf = tmp;
                        abuf.clear();
                    }

                    wresult = afc.write(wbuf, wlocation);

                    wlocation += bufsize;
                }
            }
        } catch (EndBlockException e){
            //do nothing
        }
        while (wresult != null && !wresult.isDone()) {
            //Waiting
        }
    }

    private static void mergeblocks(FileBlock a, FileBlock b, AsynchronousFileChannel afc,
                                    int bufsize) throws IOException {
        Future wresult = null;
        long TotalBlockSize = a.getBlocksize() + b.getBlocksize();
        long written = 0;
        long wlocation = a.getStartlocation();
        if (bufsize > TotalBlockSize){
            bufsize = Math.toIntExact(TotalBlockSize);
        }
        ByteBuffer abuf = ByteBuffer.allocate(bufsize);
        ByteBuffer wbuf = ByteBuffer.allocate(bufsize);
        try {
            a.PrepareBlock();
            b.PrepareBlock();
            while (true) {
                if (a.peek() <= b.peek()) {
                    abuf.asIntBuffer().put(a.pop());
                } else {
                    abuf.asIntBuffer().put(b.pop());
                }

                if (!abuf.hasRemaining()) {
                    while (wresult != null && !wresult.isDone()) {
                        //Waiting
                    }
                    wbuf.clear();
                    abuf.flip();

                    if (written + bufsize >= TotalBlockSize) {
                        wbuf = abuf;
                        abuf = ByteBuffer.allocate(toIntExact(TotalBlockSize - written));
                    } else {
                        ByteBuffer tmp;
                        tmp = wbuf;
                        wbuf = abuf;
                        abuf = tmp;
                        abuf.clear();
                    }

                    wresult = afc.write(wbuf, wlocation);

                    wlocation += bufsize;
                }
            }
        } catch (EndBlockException e){
            //do nothing
        }
        FileBlock f;
        if (a.isdone()){
            f = a;
        } else {
            f = b;
        }
        try {
            while (true) {
                abuf.asIntBuffer().put(f.pop());

                if (!abuf.hasRemaining()) {
                    while (wresult != null && !wresult.isDone()) {
                        //Waiting
                    }
                    wbuf.clear();
                    abuf.flip();

                    if (written + bufsize >= TotalBlockSize) {
                        wbuf = abuf;
                        abuf = ByteBuffer.allocate(toIntExact(TotalBlockSize - written));
                    } else {
                        ByteBuffer tmp;
                        tmp = wbuf;
                        wbuf = abuf;
                        abuf = tmp;
                        abuf.clear();
                    }

                    wresult = afc.write(wbuf, wlocation);

                    wlocation += bufsize;
                }
            }
        } catch (EndBlockException e){
            //do nothing
        }
        while (wresult != null && !wresult.isDone()) {
            //Waiting
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