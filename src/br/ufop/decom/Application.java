package br.ufop.decom;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.concurrent.Future;

import implementations.dm_kernel.user.JCL_FacadeImpl;
import interfaces.kernel.JCL_facade;
import interfaces.kernel.JCL_result;

/**
 * @author Michael Pacheco
 * 
 *         This application runs a distributed sorting. All hosts generates a
 *         collection of integers in a given range. The integers are sorted
 *         using a TreeMap. Therefore, all integers are sorted inserted. When
 *         all hosts ends, the main {@code Application} merge all sorted maps
 *         into a unique sorted map. <b>This version doesn't use the JCL global
 *         objects</b>. The other version {@code ApplicationSharedMap} does.
 *
 */
public class Application {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        
        final long INTEGERS_IN_1TB = 274877906944l;
        
        JCL_facade jcl = JCL_FacadeImpl.getInstance();

        Application.register(Sorter.class);

        Object[] sortArguments = {
                // Number of generated random numbers
                INTEGERS_IN_1TB,

                // Lower bound (included)
                0,

                // Upper bound (excluded)
                (int) 1e3 };

        // For all hosts generate random numbers and sort them
        // But before we will start a timer.
        System.out.println("\nStarting cluster sort...");
        long startTime = System.currentTimeMillis();
        List<Future<JCL_result>> executeAll = jcl.executeAll(Sorter.class.getSimpleName(), "sort", sortArguments);
        // Wait for all hosts end sorting
        List<JCL_result> results = jcl.getAllResultBlocking(executeAll);
        long endTime = System.currentTimeMillis();
        long deltaTime = endTime - startTime;
        Application.printElapsedTime(deltaTime, "finished!");

        System.out.println("Merging maps...");

        // Store the sorted collections returned by all hosts
        final int numberOfHosts = jcl.getDevices().size();
        ArrayList<SortedMap<Long, Long>> retrievedSortedMaps = new ArrayList<>(numberOfHosts);
        startTime = System.currentTimeMillis();
        // Retrieve returned maps and check for errors
        for (JCL_result jcl_result : results)
            try {
                retrievedSortedMaps.add((SortedMap<Long, Long>) jcl_result.getCorrectResult());
            } catch (ClassCastException e) {
                jcl_result.getErrorResult().printStackTrace();
            }

        Application.mergeAll(retrievedSortedMaps);
        
        endTime = System.currentTimeMillis();
        deltaTime = endTime - startTime;
        
        Application.printElapsedTime(deltaTime, "finished!");
        
        Application.cleanupJCLandQuit(jcl, null, 0);
    }
    
    private static void printElapsedTime (long deltaTime, String message) {
        Date date = new Date(deltaTime);
        DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        System.out.println(String.format("%s\nTotal time: %s\n", message, formatter.format(date)));
    }

    @SuppressWarnings("unused")
    private static void printIndividualMap(SortedMap<Integer, Long> sortedMap) {
        sortedMap.forEach((number, frequency) -> {
            for (int i = 0; i < frequency; i++)
                System.out.print(number + " ");
        });
        System.out.println();
    }

    /**
     * Register a given class in JCL cluster and exit if there are errors.
     * 
     * @param toRegister
     *            the class to register
     */
    private static void register(Class<?> toRegister) {
        JCL_facade jcl = JCL_FacadeImpl.getInstance();
        if (!jcl.register(toRegister, toRegister.getSimpleName()))
            Application.cleanupJCLandQuit(jcl,
                    String.format("Failed to register \'%s.class\'", toRegister.getSimpleName()), 1);
    }

    private static void cleanupJCLandQuit(JCL_facade jcl, String errorMessage, int returnCode) {
        if (errorMessage != null)
            System.err.println(errorMessage);
        jcl.cleanEnvironment();
        jcl.destroy();
        System.exit(returnCode);
    }

    private static SortedMap<Long, Long> mergeAll(ArrayList<SortedMap<Long, Long>> retrievedSortedMaps) {
        SortedMap<Long, Long> finalMap = new TreeMap<>();

        for (SortedMap<Long, Long> sortedMap : retrievedSortedMaps) {
            sortedMap.forEach((number, frequency) -> {
                if (finalMap.containsKey(number))
                    finalMap.put(number, finalMap.get(number) + frequency);
                else
                    finalMap.put(number, frequency);
            });
        }

        return finalMap;
    }

}
