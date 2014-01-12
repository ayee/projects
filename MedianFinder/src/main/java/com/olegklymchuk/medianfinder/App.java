package com.olegklymchuk.medianfinder;

public class App
{
    public static void main( String[] args ) {

        if(args.length != 1) {

            System.out.println("Invalid number of arguments");
            printUsage();
            return;
        }

        try {

            String[] strInput = args[0].split("\\s*,\\s*");
            int[] input = new int[strInput.length];
            int index = 0;
            for(String s : strInput)
                input[index++] = Integer.parseInt(s);

            System.out.println("Median == " + MedianFinder.getMedian(input));

        }catch(Throwable t) {

            System.out.println("Invalid argument");
            printUsage();
        }
    }

    private static void printUsage() {

        System.out.println("Please, enter comma-separated list of numbers (enclosed into quotes) to get its median");
        System.out.println("Example: \"2, 3, 2, 22, 1, 1, 1, 1, 1, 1, 4, 4, 4, 4, 5, 5, 8, 8, 8, 9, 6, 11, 100\"");
    }
}
