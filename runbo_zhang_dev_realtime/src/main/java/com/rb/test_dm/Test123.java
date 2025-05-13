package com.rb.test_dm;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

/**
 * @Package com.rb.test_dm.Test123
 * @Author runbo.zhang
 * @Date 2025/5/13 8:58
 * @description:
 */
public class Test123 {
    public static void main(String[] args) {
        JSONObject object = new JSONObject();
        object.put("a", "a");
        object.put("b", "a");
        object.put("c", null);
        System.out.println("1"+object);
        System.out.println(JSON.toJSONString(object, SerializerFeature.WriteMapNullValue));
    }
}
