package uk.ac.cam.mtd36.fjava.tick0;

public class Main {
        public static void main(String[] args) throws Exception {
            long startTime = System.currentTimeMillis();
            ExternalSort.sort("test-suite/test" + 17 + "a.dat", "test-suite/test" + 17 + "b.dat");

            //for (int i=1; i<=17; i++) {
            //    System.out.println("FILE " + i);
            //    ExternalSort.sort("test-suite/test" + i + "a.dat", "test-suite/test" + i + "b.dat");
            //}


            long stopTime = System.currentTimeMillis();
            long elapsedTime = stopTime - startTime;
            System.out.println(elapsedTime);
        }

}
