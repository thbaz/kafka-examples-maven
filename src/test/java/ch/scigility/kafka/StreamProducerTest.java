import org.junit.Test;
import static org.junit.Assert.assertEquals;
import ch.scigility.kafka.StreamProducer;
import ch.scigility.kafka.Consumer;
import java.io.IOException;

public class StreamProducerTest {
    @Test
    public void evaluateOther() {


          Thread thread = new Thread(new Runnable() {
              @Override
              public void run() {
                  System.out.println("Running");
                  try {
                    Consumer.main();
                  } catch (IOException e) {
                      // Do something here
                  }
              }
          });
          thread.start();
          int runFor = 10*1000;
          int sleepFor = 1000;
          long endTimeMillis = System.currentTimeMillis() + runFor;
          while (thread.isAlive()) {
              if (System.currentTimeMillis() > endTimeMillis) {
                  System.out.println("The End");
                  break;
              }
          }
    }
    //@Before
    //@After
    //@BeforeClass
    //@AfterClass
}
