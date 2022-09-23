package com.github.fppt.jedismock;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
import java.io.Closeable;

/**
 * Created by Xiaolu on 2015/4/21.
 */
public class Utils {

    public static void closeQuietly(Closeable closeable) {
        try {
            closeable.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static long convertToLong(String value){
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR value is not an integer or out of range");
        }
    }

    public static byte convertToByte(String value){
        try {
            byte bit = Byte.parseByte(value);
            if (bit != 0 && bit != 1) {
                throw new NumberFormatException();
            }
            return bit;
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR bit is not an integer or out of range");
        }
    }

    public static int convertToNonNegativeInteger(String value){
        try {
            int pos = Integer.parseInt(value);
            if(pos < 0) throw new NumberFormatException("Int less than 0");
            return pos;
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR bit offset is not an integer or out of range");
        }
    }

    public static int convertToInteger(String value){
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR bit offset is not an integer or out of range");
        }
    }

    public static double convertToDouble(String value){
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            throw new WrongValueTypeException("ERR bit offset is not a double or out of range");
        }
    }

    public static String createRegexFromGlob(String glob)
    {
        StringBuilder out = new StringBuilder("^");
        for(int i = 0; i < glob.length(); ++i)
        {
            final char c = glob.charAt(i);
            switch(c)
            {
                case '*':
                    out.append(".*");
                    break;
                case '?':
                    out.append('.');
                    break;
                case '.':
                    out.append("\\.");
                    break;
                case '\\':
                    out.append("\\\\");
                    break;
                case '{':
                    out.append("\\{");
                    break;
                default:
                    out.append(c);
            }
        }
        out.append('$');
        return out.toString();
    }
}
