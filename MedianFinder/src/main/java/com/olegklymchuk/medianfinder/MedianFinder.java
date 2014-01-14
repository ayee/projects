package com.olegklymchuk.medianfinder;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

/**
 * This class contains single static method to search median of the input collection
 *
 */

 public class MedianFinder {


    /**
     * Returns the median of the given int array
     *
     * @param input the array for which the median should be found
     * @return the median for a given input array
     * @throws NullPointerException if the input array is null
     * @throws RuntimeException if the input array is empty
     *
     */
    public static int getMedian(int[] input) {

        //use TreeMap since it keeps key/value pairs in sorted order
        TreeMap<Integer, Integer> tm = new TreeMap<Integer, Integer>();


        //fill tree map
        for (Integer i : input) {

            Integer value = tm.get(i);

            if (value == null)
                tm.put(i, 1);
            else
                tm.put(i, value + 1);
        }


        //find median
        int pos = 0;
        int middlePos = (input.length + 1) / 2;
        Iterator<Map.Entry<Integer, Integer>> it = tm.entrySet().iterator();
        while (it.hasNext()) {

            Map.Entry<Integer, Integer> entry = it.next();

            pos += entry.getValue();

            if (pos >= middlePos)
                return entry.getKey();

        }

        throw new RuntimeException("Invalid state!!!");
    }

}
