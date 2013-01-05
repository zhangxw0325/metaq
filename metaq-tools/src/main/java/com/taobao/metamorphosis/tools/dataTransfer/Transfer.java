/**
 * $Id: Transfer.java 2 2013-01-05 08:09:27Z shijia $
 */
package com.taobao.metamorphosis.tools.dataTransfer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.taobao.metamorphosis.Message;
import com.taobao.metamorphosis.exception.InvalidMessageException;
import com.taobao.metamorphosis.utils.MessageUtils;
import com.taobao.metamorphosis.utils.MessageUtils.DecodedMessage;
import com.taobao.metaq.commons.MetaUtil;


public class Transfer {
    private static File findFile(final String path) {
        File dir = new File(path);
        File[] files = dir.listFiles();
        if (files != null) {
            if (files.length > 0) {
                // ascending order
                Arrays.sort(files);
                return files[files.length - 1];
            }
        }
        return null;
    }


    private static ByteBuffer buildMapedByteBuffer(final File file) throws IOException {
        FileChannel fileChannel = new RandomAccessFile(file, "r").getChannel();
        MappedByteBuffer mappedByteBuffer = fileChannel.map(MapMode.READ_ONLY, 0, file.length());
        return mappedByteBuffer;
    }


    private static String pickupTopicFromPath(final String path) {
        int start = path.lastIndexOf("/");
        int end = path.lastIndexOf("-");
        return path.substring(start + 1, end);
    }

    private static int MaxMessageSize = 1024 * 1024 * 2;


    private static Message parseMessage(final String topic, final ByteBuffer byteBuffer)
            throws InvalidMessageException {
        int prevPos = byteBuffer.position();
        int length = byteBuffer.getInt();
        if (length <= MessageUtils.HEADER_LEN || length > MaxMessageSize) {
            return null;
        }

        int totalLength = length + MessageUtils.HEADER_LEN;
        byte[] data = new byte[totalLength];
        byteBuffer.position(prevPos);
        byteBuffer.get(data);
        DecodedMessage decodedMessage = MessageUtils.decodeMessage(topic, data, 0);
        return decodedMessage.message;
    }


    public static void main(String[] args) {
        if (args.length != 1) {
            System.out.println("Useage: input path");
            return;
        }

        String path = args[0];
        File file = findFile(path);
        String topic = pickupTopicFromPath(path);
        System.out.println(file.getName() + "  " + topic + " "
                + MetaUtil.timeMillisToHumanString(file.lastModified()));

        long msgTotal = 0;

        List<Message> listMsg = new ArrayList<Message>(100000);

        try {
            ByteBuffer byteBuffer = buildMapedByteBuffer(file);
            while (true) {
                Message message = parseMessage(topic, byteBuffer);
                if (message == null)
                    break;

                listMsg.add(message);
                msgTotal++;

                if ((msgTotal % 10000) == 0) {
                    System.out.println("load message total " + msgTotal);
                }

                // System.out.println(message.getTopic() + "\t" +
                // message.getAttribute() + "\t"
                // + message.getData().length + "\t" + msgTotal);
            }
        }
        catch (InvalidMessageException e) {
            e.printStackTrace();
        }
        catch (IOException e) {
            e.printStackTrace();
        }

        int index = listMsg.size() > 100000 ? (listMsg.size() - 100000) : 0;
        System.out.println("load message total num " + listMsg.size() + " " + index);
        // for (int i = index; i < listMsg.size(); i++) {
        //
        // }
    }

}
