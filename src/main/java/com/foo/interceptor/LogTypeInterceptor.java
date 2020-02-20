package com.foo.interceptor;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * Created by Administrator on 2019/1/18 0018.
 */
public class LogTypeInterceptor implements Interceptor {
    @Override
    public void initialize() {
    }
    @Override
    public Event intercept(Event event) {
        // 1 获取 flume 接收消息头
        Map<String, String> headers = event.getHeaders();
        // 2 获取 flume的body  body为json 数据数组

        byte[] json = event.getBody();

        // 将 json 数组转换为字符串

        String jsonStr = new String(json);
        String logType = "";
// startLog

//        判断日志类型，start为启动日志，其余11种为事件日志

        if (jsonStr.contains("start")) {
            logType = "start";
        }
        // eventLog
        else {
            logType = "event";
        }
        // 3 将日志类型存储到 flume 头中
        headers.put("logType", logType);
        return event;
    }
    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> interceptors = new ArrayList<>();
        for (Event event : events) {
            Event interceptEvent = intercept(event);
            interceptors.add(interceptEvent);
        }
        return interceptors;
    }




    @Override
    public void close() {
    }
    public static class Builder implements Interceptor.Builder {
        public Interceptor build() {
            return new LogTypeInterceptor();
        }
        @Override
        public void configure(Context context) {
        }
    } }