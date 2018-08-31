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

import implementations.sm_kernel.JCL_FacadeImpl;
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

	public static void main(String[] args) {
		JCL_facade jcl = JCL_FacadeImpl.getInstance();

		Application.register(Sorter.class);

		Object[] sortArguments = {
				// Number of generated random numbers
				(int) 1e7,

				// Lower bound (included)
				0,

				// Upper bound (excluded)
				10 };

		// For all hosts generate random numbers and sort them
		// But before we will start a timer.
		long startTime = System.currentTimeMillis();
		List<Future<JCL_result>> executeAll = jcl.executeAllCores(Sorter.class.getSimpleName(), "sort", sortArguments);
		// Wait for all hosts end sorting
		List<JCL_result> results = jcl.getAllResultBlocking(executeAll);
		long endTime = System.currentTimeMillis();
		long deltaTime = endTime - startTime;
		System.out.println();
		
		Date date = new Date(deltaTime);
		DateFormat formatter = new SimpleDateFormat("HH:mm:ss.SSS");
		formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
		System.out.println(String.format("The sorting was terminated! Total time: %s\n", formatter.format(date)));

		// Store the sorted collections returned by all hosts
		final int numberOfHosts = jcl.getDevices().size();
		
		ArrayList<SortedMap<Integer, Long>> retrievedSortedMaps = new ArrayList<>(numberOfHosts);

		// Retrieve returned maps and check for errors
		for (JCL_result jcl_result : results) {
			if (jcl_result.getCorrectResult() == null)
				jcl_result.getErrorResult().printStackTrace();

			@SuppressWarnings("unchecked")
			SortedMap<Integer, Long> sortedMap = (SortedMap<Integer, Long>) jcl_result.getCorrectResult();

			retrievedSortedMaps.add(sortedMap);
		}

		Application.mergeAll(retrievedSortedMaps);

		Application.cleanupJCLandQuit(jcl, null, 0);
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
	 * @param toRegister the class to register
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

	private static SortedMap<Integer, Long> mergeAll(ArrayList<SortedMap<Integer, Long>> sortedMaps) {
		SortedMap<Integer, Long> finalMap = new TreeMap<>();

		for (SortedMap<Integer, Long> sortedMap : sortedMaps) {
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
