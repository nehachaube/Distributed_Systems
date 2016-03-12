package crane.partition;

import java.util.Random;

import crane.tuple.ITuple;

public class RandomPartitionStrategy implements IPartitionStrategy {

    private static final long serialVersionUID = 1L;
    // Random here is better than ThreadLocalRandom, think about why
    private Random random = new Random();

    @Override
    public int partition(ITuple tuple, int numTasks) {
        return random.nextInt(numTasks);
    }

}
