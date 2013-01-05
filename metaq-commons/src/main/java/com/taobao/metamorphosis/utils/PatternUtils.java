package com.taobao.metamorphosis.utils;

import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class PatternUtils {
    /**
     * 与pattern.split类似，但是不消除结果中的空字符串
     * 
     * @param pattern
     * @param input
     * @return
     */
    public static String[] split(final Pattern pattern, final CharSequence input) {
        return split(pattern, input, 0);
    }


    /**
     * 与pattern.split类似，但是不消除结果中的空字符串
     * 
     * @param pattern
     * @param input
     * @param limit
     * @return
     */
    public static String[] split(final Pattern pattern, final CharSequence input, final int limit) {
        int index = 0;
        final boolean matchLimited = limit > 0;
        final ArrayList<String> matchList = new ArrayList<String>();
        final Matcher m = pattern.matcher(input);

        // Add segments before each match found
        while (m.find()) {
            if (!matchLimited || matchList.size() < limit - 1) {
                final String match = input.subSequence(index, m.start()).toString();
                matchList.add(match);
                index = m.end();
            }
            else if (matchList.size() == limit - 1) { // last one
                final String match = input.subSequence(index, input.length()).toString();
                matchList.add(match);
                index = m.end();
            }
        }

        // If no match was found, return this
        if (index == 0) {
            return new String[] { input.toString() };
        }

        // Add remaining segment
        if (!matchLimited || matchList.size() < limit) {
            matchList.add(input.subSequence(index, input.length()).toString());
        }

        // Construct result
        final int resultSize = matchList.size();
        // if (limit == 0) {
        // while (resultSize > 0 && matchList.get(resultSize - 1).equals("")) {
        // resultSize--;
        // }
        // }
        final String[] result = new String[resultSize];
        return matchList.subList(0, resultSize).toArray(result);
    }

}
