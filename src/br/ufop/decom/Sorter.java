package br.ufop.decom;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.SortedMap;
import java.util.TimeZone;
import java.util.TreeMap;

/**
 * @author Michael Pacheco Sorts a randomly generated collection of integers.
 */
public class Sorter {

    /**
     * Sorts a randomly generated collection of {@code amount} integers in the
     * [{@code lowerBound}, {@code upperBound}) range
     * 
     * @param amount
     *            the number of integers to be generated
     * @param lowerBound
     *            the lower possible generated integer (inclusive)
     * @param upperBound
     *            the higher possible generated integer (exclusive)
     * @return the generated numbers sorted in a ordered map where the keys are the
     *         generated number and the value is the frequency of this number
     */
    public static SortedMap<Long, Long> sort(long amount, int lowerBound, int upperBound) {
        System.out.println("\nStarting local sorting!");
        System.out.println(String.format(Locale.US, "Sorting %,d integers in [%d, %,d) range...", amount, lowerBound, upperBound));

        // A map with sorted keys
        SortedMap<Long, Long> sortedNumbers = new TreeMap<>();

        long startTime = System.currentTimeMillis();

        for (long i = 0l; i < amount; i++) {
            final long randomNumber = (long) (lowerBound + (Math.random() * (upperBound - lowerBound)));

            // The frequency of the generated random number in numbers map
            Long randomNumberFrequency = null;

            if ((randomNumberFrequency = sortedNumbers.get(randomNumber)) != null)
                sortedNumbers.put(randomNumber, randomNumberFrequency + 1l);
            else
                sortedNumbers.put(randomNumber, 1l);
        }

        long endTime = System.currentTimeMillis();
        long deltaTime = endTime - startTime;

        Date date = new Date(deltaTime);
        DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        System.out.println(String.format("The sorting was terminated! Total time: %s\n", formatter.format(date)));

        Runtime.getRuntime().gc();
        Runtime.getRuntime().freeMemory();

        return sortedNumbers;
    }
}
