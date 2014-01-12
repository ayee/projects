package com.olegklymchuk.medianfinder;

import static org.junit.Assert.*;
import org.junit.Test;

public class AppTest {

    @Test(expected = RuntimeException.class)
    public void testNullInputShouldThrow() {

        MedianFinder.getMedian(null);
    }

    @Test(expected = RuntimeException.class)
    public void testEmptyInputShouldThrow() {

        MedianFinder.getMedian(new int[]{});
    }

    @Test
    public void testSingeElementInput() {

        assertEquals(1, MedianFinder.getMedian(new int[] {1}));
    }

    @Test
    public void testInputWithSameNumbers() {

        assertEquals(9, MedianFinder.getMedian(new int[] {9, 9, 9, 9}));
    }

    @Test
    public void testOddAmountOfNumbersInInput() {

        assertEquals(23, MedianFinder.getMedian(new int[] {3, 13, 7, 5, 21, 23, 39, 23, 40, 23, 14, 12, 56, 23, 29}));
    }

    @Test
    public void testEvenAmountOfNumbersInInput() {

        assertEquals(22, MedianFinder.getMedian(new int[] {3, 13, 7, 5, 21, 23, 23, 40, 23, 14, 12, 56, 23, 29}));
    }

}
