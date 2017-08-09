import org.junit.Test;
import static org.junit.Assert.assertEquals;
import ch.scigility.kafka.StreamProcessor;
import ch.scigility.kafka.StreamProducer;
import ch.scigility.kafka.Consumer;
import ch.scigility.kafka.canonical.Partners;
import java.io.IOException;

public class StreamProducerTest {
    // @Test
    // public void ConsumerTopicTest() {
    //   System.out.println("ConsumerTopicTest:");
    //   Thread thread = new Thread(new Runnable() {
    //       @Override
    //       public void run() {
    //
    //           try {
    //             Consumer.main();
    //           } catch (IOException e) {
    //               // Do something here
    //           }
    //       }
    //   });
    //   thread.start();
    //   int runFor = 30*1000;
    //   int sleepFor = 1000;
    //   long endTimeMillis = System.currentTimeMillis() + runFor;
    //   while (thread.isAlive()) {
    //       if (System.currentTimeMillis() > endTimeMillis) {
    //           System.out.println("end of this batch");
    //           break;
    //       }
    //   }
    // }
    // @Test
    // public void StreamProcessorTest() {
    //   System.out.println("ConsumerTopicTest:");
    //   Thread thread = new Thread(new Runnable() {
    //       @Override
    //       public void run() {
    //
    //           try {
    //             StreamProcessor.main();
    //           } catch (IOException e) {
    //               // Do something here
    //           }
    //       }
    //   });
    //   thread.start();
    //   int runFor = 30*1000;
    //   int sleepFor = 1000;
    //   long endTimeMillis = System.currentTimeMillis() + runFor;
    //   while (thread.isAlive()) {
    //       if (System.currentTimeMillis() > endTimeMillis) {
    //           System.out.println("end of this batch");
    //           break;
    //       }
    //   }
    // }

    @Test
    public void StreamProcessorTest() {
      System.out.println("ConsumerTopicTest:");
      Thread thread = new Thread(new Runnable() {
          @Override
          public void run() {
              try {
                Partners.main();
              } catch (IOException e) {
                  // Do something here
              }
          }
      });
      thread.start();
      int runFor = 5*60*1000;
      int sleepFor = 1000;
      long endTimeMillis = System.currentTimeMillis() + runFor;
      while (thread.isAlive()) {
          if (System.currentTimeMillis() > endTimeMillis) {
              System.out.println("end of this batch");
              break;
          }
      }
    }
    //@Before
    //@After
    //@BeforeClass
    //@AfterClass
}
