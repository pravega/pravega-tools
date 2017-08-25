/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.tools.pravegastreamstat.service;

import lombok.Getter;
import lombok.Setter;

import java.io.PrintStream;
import java.util.Map;
import java.util.UUID;

/**
 * Formatted print utilities.
 */
public class PrintHelper {

    private static final int MAX_PROCESS_LENGTH = 60;
    private static final PrintStream PRINT_STREAM = System.out;
    private static final Activator ACTIVATOR = new Activator();

    public static void block() {
        ACTIVATOR.setActive(false);
    }

    public static void unblock() {
        ACTIVATOR.setActive(true);
    }

    // region Print functions

    /**
     * Print with color.
     * @param color Print color.
     * @param toPrint Object to print
     *
     */
    public static void print(Color color, Object toPrint) {
        if (ACTIVATOR.isActive()) {
            PRINT_STREAM.format("%s%s%s", color.getAnsi(), toPrint, Color.RESET.getAnsi());
        }
    }

    /**
     * Ordinary print.
     * @param toPrint String to print
     */
    public static void print(Object toPrint) {
        if (ACTIVATOR.isActive()) {
            PRINT_STREAM.print(toPrint);
        }
    }

    /**
     * print attribute string, override with different value type.
     * @param attrName Attribute name
     * @param val Attribute value
     * @param isLast determine if it is the last attribute
     */
    public static void print(String attrName, Object val, boolean isLast) {
        print(Color.GREEN, attrName);
        print(" = ");
        print(Color.YELLOW, val);
        if (!isLast) {
            print(", ");
        } else {
            println();
            println();
        }
    }

    /**
     * Print out the segment attributes.
     * @param val The segment attribute map.
     */
    public static void print(Map<UUID, Long> val) {
        print("Attributes", val.toString(), true);
    }

    /**
     * Ordinary println.
     */
    public static void println() {
        if (ACTIVATOR.isActive()) {
            PRINT_STREAM.println();
        }
    }

    /**
     * Ordinary println.
     * @param s The object to print.
     */
    public static void println(Object s) {
            print(s);
            println();
    }

    /**
     * Println with color.
     * @param color Print color.
     * @param s     The object to print.
     */
    public static void println(Color color, Object s) {
        print(color, s);
        println();
    }

    /**
     * Ordinary format print.
     * @param s         Format string to print.
     * @param objects   Format arguments.
     */
    public static void format(String s, Object... objects) {
        print(String.format(s, objects));
    }

    /**
     * Colored format print.
     * @param color   Print color.
     * @param s       Format string to print.
     * @param objects Format arguments.
     */
    public static void format(Color color, String s, Object... objects) {
        print(color, String.format(s, objects));
    }

    // endregion

    // region Print by functionality

    /**
     * Print error.
     * @param s Error message.
     */
    public static void printError(Object s) {
        println(Color.RED, s);
        println();
    }

    /**
     * Print header of an attribute table. About what the table is.
     * @param toPrint The header to print.
     */
    public static void printHead(String toPrint) {
        print(Color.BLUE, String.format("%s: ", toPrint));
    }

    // endregion

    // region Process Indicator

    /**
     * Start a indicator about what the process is doing.
     * @param process A message about what the process is doing.
     */
    public static void processStart(String process) {
        print(Color.PURPLE, String.format("%s ", process));
    }

    /**
     * End the process indicator.
     */
    public static void processEnd() {
        print("\r");
        format("%" + MAX_PROCESS_LENGTH + "s", " ");
        print("\r");
    }

    // endregion

    /**
     * The colors to print.
     */
    public enum Color {
        RESET("\u001B[0m"),
        RED("\u001B[31m"),
        GREEN("\u001B[32m"),
        YELLOW("\u001B[33m"),
        BLUE("\u001B[34m"),
        PURPLE("\u001B[35m"),
        CYAN("\u001B[36m");

        /**
         * The ansi code to print out in color.
         */
        @Getter
        private String ansi;
        Color(String ansi) {
            this.ansi = ansi;
        }


    }

    /**
     * A static class stores the status of the print utilities.
     */
    private static class Activator {
        @Getter
        @Setter
        private boolean active = true;
    }
}
