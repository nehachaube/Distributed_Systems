package crane.task;

/**
 * CraneWorker is worker thread for Crane.
 */
public interface CraneWorker extends Runnable {
    /**
     * set the task for the worker.
     * 
     * @param comp
     */
    void setTask(Task task);

    /**
     * Terminate the worker
     */
    void terminate();
}
