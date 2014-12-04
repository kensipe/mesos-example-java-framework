package com.nfjs.tutorial.mesos.docker;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Scheduler;

/**
 * Example framework that will run a scheduler that in turn will cause
 * Docker containers to be launched.
 * <p/>
 * Source code adapted from the example that ships with Mesos.
 * Source code adapted from CodeFutures Example
 */
public class JavaDockerFramework {

    /**
     * Show command-line usage.
     */
    private static void usage() {
        String name = JavaDockerFramework.class.getName();
        System.err.println("Usage: " + name + " master-ip-and-port docker-image-name number-of-instances");
    }

    /**
     * Command-line entry point.
     * <br/>
     * Example usage: java JavaDockerFramework 127.0.0.1:5050 fedora/apache 2
     */
    public static void main(String[] args) throws Exception {

        // check command-line args
        if (args.length != 3) {
            usage();
            System.exit(1);
        }

        // If the framework stops running, mesos will terminate all of the tasks that
        // were initiated by the framework but only once the fail-over timeout period
        // has expired. Using a timeout of zero here means that the tasks will
        // terminate immediately when the framework is terminated. For production
        // deployments this probably isn't the desired behavior, so a timeout can be
        // specified here, allowing another instance of the framework to take over.
        final int frameworkFailoverTimeout = 0;

        FrameworkInfo.Builder frameworkBuilder = FrameworkInfo.newBuilder()
                .setName("JavaDockerFramework")
                .setUser("") // Have Mesos fill in the current user.
                .setFailoverTimeout(frameworkFailoverTimeout); // timeout in seconds

        if (System.getenv("MESOS_CHECKPOINT") != null) {
            System.out.println("Enabling checkpoint for the framework");
            frameworkBuilder.setCheckpoint(true);
        }

        // parse command-line args
        final String imageName = args[1];
        final int totalTasks = Integer.parseInt(args[2]);

        // create the scheduler
        final Scheduler scheduler = new JavaDockerScheduler(
                imageName,
                totalTasks
        );


        frameworkBuilder.setPrincipal("test-framework-java");
        MesosSchedulerDriver driver = new MesosSchedulerDriver(scheduler, frameworkBuilder.build(), args[0]);


        int status = driver.run() == Status.DRIVER_STOPPED ? 0 : 1;

        // Ensure that the driver process terminates.
        driver.stop();

        System.exit(status);
    }
}
