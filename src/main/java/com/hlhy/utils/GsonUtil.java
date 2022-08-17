package com.hlhy.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

/**
 * Created by Administrator on 2017/6/17.
 */

public class GsonUtil {
        private static Gson gson = null;
        static {
            if (gson == null) {
                gson = new Gson();
            }
        }

        private GsonUtil() {
        }

        /**
         * 转成json
         *
         * @param object
         * @return
         */
        public static String GsonString(Object object) {
            String gsonString = null;
            if (gson != null) {
                gsonString = gson.toJson(object);
            }
            return gsonString;
        }

        /**
         * 转成bean
         *
         * @param gsonString
         * @param cls
         * @return
         */
        public static <T> T GsonToBean(String gsonString, Class<T> cls) {
            T t = null;
            if (gson != null) {
                t = gson.fromJson(gsonString, cls);
            }
            return t;
        }

        /**
         * 转成list
         *
         * @param gsonString
         * @param cls
         * @return
         */
        public static <T> List<T> GsonToList(String gsonString, Class<T> cls) {
            List<T> list = null;
            if (gson != null) {
                list = gson.fromJson(gsonString, new TypeToken<List<T>>() {}.getType());
            }
            return list;
        }

        /**
         * 转成list中有map的
         *
         * @param gsonString
         * @return
         */
        public static <T> List<Map<String, T>> GsonToListMaps(String gsonString, Class<T> cls) {
            List<Map<String, T>> list = null;
            if (gson != null) {
                list = gson.fromJson(gsonString, new TypeToken<List<Map<String, T>>>() {}.getType());
            }
            return list;
        }

        /**
         * 转成map的
         *
         * @param gsonString
         * @return
         */
        public static <T> Map<String, T> GsonToMaps(String gsonString, Class<T> cls) {
            Map<String, T> map = null;
            if (gson != null) {
                map = gson.fromJson(gsonString, new TypeToken<Map<String, T>>() {
                }.getType());
            }
            return map;
        }
        public static void main(String[] args ) {
            String param="{\"startcts\": \"20180201\",\"endcts\": \"20180811\",\"newstype\": \"1\",\"mode\": \"1\",\"filter\": [{\"columnrange\": \"1\",\"is\":\"2\",\"keys\": \"N\"}],\"result\": \"2\",\"tp1stid\": \"tiyu,xiaohua,yule\",\"myinfo\": {\"rowkey\": \"12345678aa5663123\"},\"callurl\":\"http://172.18.250.87:8800/main\"}";
            Object gsonToBean = GsonToBean(param,Object.class);
            System.out.println(gsonToBean.toString());
            //JsonObject jsonObject = (JsonObject) new JsonParser().parse(param);
            JsonElement jsonElement = new JsonParser().parse(param);
            //JsonElement这个元素可以是一个Json(JsonObject)、可以是一个数组(JsonArray)、可以是一个Java的基本类型(JsonPrimitive)、当然也可以为null(JsonNull);
            if(!jsonElement.isJsonObject()){
    			return;
    		}
            JsonObject jsonObject = (JsonObject)jsonElement;
            System.out.println("startcts:" + jsonObject.get("startcts").getAsInt());
            System.out.println("columnrange:" + jsonObject.get("filter").getAsJsonArray().get(0).getAsJsonObject().get("columnrange").getAsString());
            System.out.println("rowkey:" + jsonObject.get("myinfo").getAsJsonObject().get("rowkey").getAsString());
            String param1="{'startcts': 'startcts'}";
            
            
            Map<String, String> gsonToMaps = GsonToMaps(param1,String.class);
            System.out.println(gsonToMaps.get("startcts"));
            
            List<String> list = new ArrayList<String>();
            list.add("Q100");
            list.add("a00000");
            list.add("b00002");
            list.add("c00003");
            String json = gson.toJson(list);
            System.out.println(json);
          
		}
}
