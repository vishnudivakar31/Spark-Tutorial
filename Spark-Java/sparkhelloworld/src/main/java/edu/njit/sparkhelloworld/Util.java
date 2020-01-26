package edu.njit.sparkhelloworld;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;

public class Util {
    private static Set<String> boringWords = new HashSet<>();

    static {
        InputStream inputStream = Main.class.getResourceAsStream("/subtitles/boringwords.txt");
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        bufferedReader.lines().forEach(boringWords::add);
    }

    public static boolean isBoring(String word) {
        return boringWords.contains(word);
    }

    public static boolean isNotBoring(String word) {
        return !isBoring(word);
    }
}
