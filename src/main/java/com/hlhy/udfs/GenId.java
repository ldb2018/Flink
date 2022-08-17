package com.hlhy.udfs;

import org.apache.flink.table.functions.ScalarFunction;
public class GenId extends ScalarFunction {

    private static final char[] CHAR_32 = {'A', 'B', 'C', 'D', 'E', 'F', 'G',
            'H', 'J', 'K', 'L', 'M', 'N', 'P', 'Q', 'R', 'S', 'T', 'U', 'V',
            'W', 'X', 'Y', 'Z', '2', '3', '4', '5', '6', '7', '8', '9'};

    public String evaluate(int len, double random) {
        StringBuilder s = new StringBuilder();
        for (int i = 0; i < len; i++) {
            double rand = Math.random() + random;
            s.append(CHAR_32[((int) (rand * 1000000.0D) % CHAR_32.length)]);
        }
        return s.toString();
    }

    public String evaluate(int len) {
        return evaluate(len, 0);
    }

    public String eval(Integer len){
        return evaluate(len);
    }
}