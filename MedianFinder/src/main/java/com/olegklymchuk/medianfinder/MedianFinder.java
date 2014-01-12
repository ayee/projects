package com.olegklymchuk.medianfinder;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class MedianFinder {

    public static int getMedian(int[] input) {

        if(input == null || input.length == 0)
            throw new RuntimeException("null or empty input is not permitted!!!");

        //use TreeMap since it keeps key/value pairs in sorted order
        TreeMap<Integer, Integer> tm = new TreeMap<Integer, Integer>();

        //fill tree map
        for(Integer i : input) {
            Integer value = tm.get(i);
            if(value == null)
                tm.put(i, 1);
            else
                tm.put(i, ++value);
        }

        //find median
        int pos = 0;
        int middlePos = (input.length + 1) / 2;
        Iterator<Map.Entry<Integer, Integer>> it = tm.entrySet().iterator();
        while (it.hasNext()) {

            Map.Entry<Integer, Integer> entry = it.next();

            pos += entry.getValue();

            if(pos > middlePos)
                return entry.getKey();

            if(pos == middlePos)
                return (((input.length % 2 != 0) || !it.hasNext()) ? entry.getKey() : ((entry.getKey() + it.next().getKey()) / 2));

        }

        throw new RuntimeException("Invalid state!!!");
    }

}
