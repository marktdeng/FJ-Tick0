package uk.ac.cam.mtd36.fjava.tick0;

import java.util.Arrays;
import java.util.LinkedList;

public class Main {
        public static void main(String[] args) throws Exception {
            long startTime = System.currentTimeMillis();

            //System.out.println(Arrays.toString(ExternalSort.read("test-suite/test6a.dat",0,100)));
            //ExternalSort.sort("test-suite/test1a.dat","test-suite/test1b.dat");
            //ExternalSort.sort("test-suite/test2a.dat","test-suite/test2b.dat");
            //ExternalSort.sort("test-suite/test3a.dat","test-suite/test3b.dat");
            //ExternalSort.sort("test-suite/test4a.dat","test-suite/test4b.dat");
            //ExternalSort.sort("test-suite/test5a.dat","test-suite/test5b.dat");
            //ExternalSort.sort("test-suite/test6a.dat","test-suite/test6b.dat");
            //ExternalSort.sort("test-suite/test7a.dat","test-suite/test7b.dat");
            ExternalSort.sort("test-suite/test15a.dat","test-suite/test15b.dat");

            //ExternalSort.printfile("test-suite/test15a.dat", 65536);

            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            //System.out.println(elapsedTime);
        }

}
