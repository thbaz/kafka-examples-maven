import org.junit.Test;
import static org.junit.Assert.assertEquals;
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
                    StreamProducer.main();
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
              try {
                  System.out.println("Running: "+((endTimeMillis - System.currentTimeMillis())/1000) +" s");
                  Thread.sleep(sleepFor);
              }
              catch (InterruptedException t) {}
          }
    }
    //@Before
    //@After
    //@BeforeClass
    //@AfterClass
}
