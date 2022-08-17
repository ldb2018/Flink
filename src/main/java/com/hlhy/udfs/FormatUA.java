package com.hlhy.udfs;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author admin
 * @Title: FormatOaid
 * @ProjectName formatOaidUdf
 * @Description: TODO
 * @date 2020/3/2511:52
 */

@Description(name = "formatua", value = "_FUNC_(string) - Returns a  string ", extended = "Example:\n >SELECT _FUNC_('Mozilla/5.0 (iPhone; CPU iPhone OS 13_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148') FROM src LIMIT 1;\n  iPhone;CPUiPhoneOS13_6likeMacOSX")
public class FormatUA extends GenericUDF {
    private static final Pattern pattern = Pattern.compile("\\(.*?\\)");
    // Mozil开头
    private static final Pattern pattern1 = Pattern.compile("^(iphone|ipad);cpuiphoneos(\\d*_\\d*_?\\d*).*");
    // Aweme开头 Video开头 News开头
    private static final Pattern pattern2 = Pattern.compile("^(iphone|ipad);(iphoneos|ios)(\\d*\\.\\d*\\.?\\d*).*");
    // apple开头
    private static final Pattern pattern3 = Pattern.compile("^apple.*__ios__(\\d*\\.\\d*\\.?\\d*)__.*");

    public static String handleUserAgent(String ua) {
        if (StringUtils.isBlank(ua)) {
            return StringUtils.EMPTY;
        }
        //ua = URLDecoder.decode(ua);
        try {
            ua = URLDecoder.decode(ua, "utf-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        try {
            if (!ua.contains("Android")) {
                return handleUserAgentDefault(ua);
            }
            String result = handleUserAgentAndroid(ua);
            if (StringUtils.isBlank(result)) {
                return handleUserAgentDefault(ua);
            }
            return result;
        } catch (Exception e) {
        }
        return StringUtils.EMPTY;
    }

    /**
     * 处理UserAgent的默认策略
     *
     * @param ua UserAgent
     * @return 处理后的UserAgent
     */
    private static String handleUserAgentDefault(String ua) {
        Matcher matcher = pattern.matcher(ua);
        if (matcher.find()) {
            ua = StringUtils.defaultString(matcher.group());
        }
        ua = ua.replaceAll(" U;", "");
        ua = ua.replaceAll("; wv", "");
        ua = ua.replaceAll("\\s\\w\\w-\\w\\w;", "");
        ua = ua.replaceAll("0.0;", "0;");
        ua = ua.replaceAll("1.0;", "1;");
        ua = ua.replaceAll("zh-cn;", "");
        ua = ua.replaceAll("zh-CN;", "");
        ua = ua.replaceAll(" ", "");
        ua = ua.replaceAll("%20", "");
        ua = ua.replaceAll("\\(", "").replaceAll("\\)", "");
        return ua;
    }

    /**
     * 处理UserAgent
     *
     * @param ua UserAgent
     * @return 处理后的UserAgent
     */
    private static String handleUserAgentAndroid(String ua) {
        ua = ua.replaceAll(" U;", "");
        ua = ua.replaceAll("; wv", "");
        ua = ua.replaceAll("\\s\\w\\w-\\w\\w;", "");
        Matcher matcher = Pattern.compile("Android\\s\\d.*?/").matcher(ua);
        if (!matcher.find()) {
            return "";
        }
        ua = matcher.group();
        String[] uas = ua.split(";");
        if (uas.length < 1) {
            return "";
        }
        String system = "";
        String model = "";
        for (String uaStr : uas) {
            if (uaStr.contains("Android")) {
                Matcher systemMatcher = Pattern.compile("Android\\s\\d+").matcher(uaStr);
                if (systemMatcher.find()) {
                    system = systemMatcher.group();
                }
            } else if (uaStr.contains("/")) {
                model = uaStr.trim().split(" ")[0];
            }
        }

        return StringUtils.isNotBlank(system) && StringUtils.isNotBlank(model) ? (system + model).replaceAll(" ", "") : StringUtils.EMPTY;
    }

    /**
     * @title: getOsversionByUa
     * @param: [uaTmp]
     * @return: java.lang.String
     * @description: 根据handleUserAgent方法获取到的第一层ua转小写之后获取iOS系统版本，大部分ua可以获取到设备是iphone还是ipad，部分ua获取不到
     * @author: zhaozhennan
     * @date: 2020/11/11 10:14
     * @version: V1.0
     */
    public static String getOsversionByUa(String uaTmp) {
        uaTmp = handleUserAgent(uaTmp);
        String iphoneOrIpad = "";
        String osVersion = "";
        Matcher matcher1 = pattern1.matcher(uaTmp.toLowerCase());
        Matcher matcher2 = pattern2.matcher(uaTmp.toLowerCase());
        Matcher matcher3 = pattern3.matcher(uaTmp.toLowerCase());
        if (matcher1.find()) {
            iphoneOrIpad = matcher1.group(1);
            osVersion = matcher1.group(2);
        } else if (matcher2.find()) {
            iphoneOrIpad = matcher2.group(1);
            osVersion = matcher2.group(3);
        } else if (matcher3.find()) {
            osVersion = matcher3.group(1);
        }
        if (StringUtils.isBlank(osVersion)) {
            return "";
        } else {
            return iphoneOrIpad + osVersion;
        }
    }

    //这个方法只调用一次，并且在evaluate()方法之前调用，该方法接收的参数是一个ObjectInspectors数组，该方法检查接收正确的参数类型和参数个数
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
// 定义函数的返回类型为java的String
        return PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(PrimitiveObjectInspector.PrimitiveCategory.STRING);
    }

    //这个方法类似evaluate方法，处理真实的参数，返回最终结果
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {

        if (deferredObjects.length != 1) {
            throw new UDFArgumentLengthException(" arguments length must be equal to 1");
        }

        String ua = deferredObjects[0].get().toString();
        return handleUserAgent(ua);
    }

    //此方法用于当实现的GenericUDF出错的时候，打印提示信息，提示信息就是该方法的返回的字符串
    @Override
    public String getDisplayString(String[] strings) {
        return "Usage: formatua(String ua)";
    }
}
