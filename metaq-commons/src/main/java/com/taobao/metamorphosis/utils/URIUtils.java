package com.taobao.metamorphosis.utils;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;


/**
 * 
 * @author ÎÞ»¨
 * @since 2011-6-22 ÏÂÎç04:04:00
 */

public class URIUtils {

    private static final String PARAMETER_SEPARATOR = "&";
    private static final String NAME_VALUE_SEPARATOR = "=";
    public static final String DEFAULT_CONTENT_CHARSET = "ISO_8859_1";


    public static Map<String, String> parseParameters(URI uri, String encoding) {
        Map<String, String> result = Collections.emptyMap();
        final String query = uri.getRawQuery();
        if (query != null && query.length() > 0) {
            result = new HashMap<String, String>();
            parse(result, new Scanner(query), encoding);
        }
        return result;
    }


    private static void parse(Map<String, String> result, Scanner scanner, String encoding) {
        scanner.useDelimiter(PARAMETER_SEPARATOR);
        while (scanner.hasNext()) {
            final String[] nameValue = scanner.next().split(NAME_VALUE_SEPARATOR);
            if (nameValue.length == 0 || nameValue.length > 2)
                throw new IllegalArgumentException("bad parameter");

            final String name = decode(nameValue[0], encoding);
            String value = null;
            if (nameValue.length == 2) {
                value = decode(nameValue[1], encoding);
            }
            result.put(name, value);
        }
    }


    private static String decode(final String content, final String encoding) {
        try {
            return URLDecoder.decode(content, encoding != null ? encoding : DEFAULT_CONTENT_CHARSET);
        }
        catch (UnsupportedEncodingException e) {
            throw new IllegalArgumentException(e);
        }
    }
}
