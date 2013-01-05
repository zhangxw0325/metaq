package com.taobao.metamorphosis.tail4j;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Date;
import java.util.Enumeration;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;

import com.taobao.metamorphosis.tail4j.Config.LogConfig;


/**
 * 用于支持断点续传的check point
 * 
 * @author boyan
 * @Date 2011-5-17
 * 
 */
public class CheckPoint {

    private final Properties props;

    private final FileChannel fc;

    static final String POSITION = "position";

    static final String LAST_MODIFY = "last_modify";


    public CheckPoint(Config config, LogConfig logConfig) throws IOException {
        super();
        final File parent = new File(config.getCheckPointPath());
        if (!parent.exists()) {
            if (!parent.mkdir()) {
                throw new IOException("mkdir failed:" + config.getCheckPointPath());
            }
        }
        final String fileName = config.getCheckPointPath() + File.separator + logConfig.checkPointName;
        this.props = new java.util.Properties();
        File file = new File(fileName);
        if (file.exists()) {
            final FileReader reader = new FileReader(file);
            this.props.load(reader);
            reader.close();
        }
        this.fc = new RandomAccessFile(file, "rw").getChannel();
    }


    private void set(String key, String value) {
        this.props.setProperty(key, value);
    }


    public String getPosition() {
        return this.get(POSITION);
    }


    public void setPosition(long pos) {
        this.set(POSITION, String.valueOf(pos));
    }


    public long getLastModify() {
        final String value = this.get(LAST_MODIFY);
        return StringUtils.isBlank(value) ? -1 : Long.parseLong(value);
    }


    public void setLastModify(long lastModify) {
        this.set(LAST_MODIFY, String.valueOf(lastModify));
    }


    private String get(String key) {
        return this.props.getProperty(key);
    }


    public String getPropsString() {
        StringBuilder sb = new StringBuilder();
        sb.append("#" + new Date().toString());
        sb.append("\r\n");
        for (Enumeration e = this.props.keys(); e.hasMoreElements();) {
            String key = (String) e.nextElement();
            String val = (String) this.props.get(key);
            key = this.saveConvert(key, true, false);
            /*
             * No need to escape embedded and trailing spaces for value, hence
             * pass false to flag.
             */
            val = this.saveConvert(val, false, false);
            sb.append(key + "=" + val);
            sb.append("\r\n");
        }
        return sb.toString();
    }


    /**
     * Convert a nibble to a hex character
     * 
     * @param nibble
     *            the nibble to convert.
     */
    private static char toHex(int nibble) {
        return hexDigit[(nibble & 0xF)];
    }

    /** A table of hex digits */
    private static final char[] hexDigit = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
                                            'F' };


    /*
     * Converts unicodes to encoded &#92;uxxxx and escapes special characters
     * with a preceding slash
     */
    private String saveConvert(String theString, boolean escapeSpace, boolean escapeUnicode) {
        int len = theString.length();
        int bufLen = len * 2;
        if (bufLen < 0) {
            bufLen = Integer.MAX_VALUE;
        }
        StringBuffer outBuffer = new StringBuffer(bufLen);

        for (int x = 0; x < len; x++) {
            char aChar = theString.charAt(x);
            // Handle common case first, selecting largest block that
            // avoids the specials below
            if (aChar > 61 && aChar < 127) {
                if (aChar == '\\') {
                    outBuffer.append('\\');
                    outBuffer.append('\\');
                    continue;
                }
                outBuffer.append(aChar);
                continue;
            }
            switch (aChar) {
            case ' ':
                if (x == 0 || escapeSpace) {
                    outBuffer.append('\\');
                }
                outBuffer.append(' ');
                break;
            case '\t':
                outBuffer.append('\\');
                outBuffer.append('t');
                break;
            case '\n':
                outBuffer.append('\\');
                outBuffer.append('n');
                break;
            case '\r':
                outBuffer.append('\\');
                outBuffer.append('r');
                break;
            case '\f':
                outBuffer.append('\\');
                outBuffer.append('f');
                break;
            case '=': // Fall through
            case ':': // Fall through
            case '#': // Fall through
            case '!':
                outBuffer.append('\\');
                outBuffer.append(aChar);
                break;
            default:
                if ((aChar < 0x0020 || aChar > 0x007e) & escapeUnicode) {
                    outBuffer.append('\\');
                    outBuffer.append('u');
                    outBuffer.append(toHex(aChar >> 12 & 0xF));
                    outBuffer.append(toHex(aChar >> 8 & 0xF));
                    outBuffer.append(toHex(aChar >> 4 & 0xF));
                    outBuffer.append(toHex(aChar & 0xF));
                }
                else {
                    outBuffer.append(aChar);
                }
            }
        }
        return outBuffer.toString();
    }


    public void save() throws IOException {
        ByteBuffer buf = ByteBuffer.wrap(this.getPropsString().getBytes());
        this.fc.position(0);
        while (buf.hasRemaining()) {
            this.fc.write(buf);
        }
        this.fc.truncate(this.fc.size());
    }


    public void close() throws IOException {
        this.save();
        this.fc.force(true);
        this.fc.close();
    }

}
