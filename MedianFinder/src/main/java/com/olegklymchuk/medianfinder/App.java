package com.olegklymchuk.medianfinder;

public class App
{
    public static void main( String[] args ) {

        try {

            System.exit(ProcessorPool.instance().getProcessor(args[0]).process(args));

        }
        catch (Throwable t) {

            System.out.println("Failed to perform requested operation due to error: " + t.getMessage());
        }
    }
}
