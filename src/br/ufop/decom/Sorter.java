package br.ufop.decom;

import java.util.SortedMap;
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
    public static SortedMap<Integer, Long> sort(int amount, int lowerBound, int upperBound) {

        // A map with sorted keys
        SortedMap<Integer, Long> sortedNumbers = new TreeMap<>();

        for (int i = 0; i < amount; i++) {
            final int randomNumber = (int) (Math.random() * upperBound);

            // The frequency of the generated random number in numbers map
            Long randomNumberFrequency = null;

            if ((randomNumberFrequency = sortedNumbers.get(randomNumber)) != null)
                sortedNumbers.put(randomNumber, randomNumberFrequency + 1);
            else
                sortedNumbers.put(randomNumber, 1l);
        }

        return sortedNumbers;
    }
}
