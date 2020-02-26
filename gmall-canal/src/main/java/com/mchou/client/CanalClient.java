package com.mchou.client;


import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.mchou.constants.GmallConstants;
import com.mchou.untils.KafkaSender;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {
    public static void main(String[] args) throws InterruptedException, InvalidProtocolBufferException {

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop106", 11111),
                "example", "", "");


        while (true) {
            //连接canal
            canalConnector.connect();

            //订阅监控的数据库
            canalConnector.subscribe("gmall2.*");

            //抓去数据
            Message message = canalConnector.get(100);

            //判断当前是否有数据更新
            if (message.getEntries().size() <= 0) {

                System.out.println("无数据。。。。");

                Thread.sleep(5000);

            } else {
                //解析message
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //判断当前的Entry类型,过滤掉类似于事务的开启与关闭操作
                    if (CanalEntry.EntryType.ROWDATA.equals(entry.getEntryType())) {

                        //解析entry
                        String tableName = entry.getHeader().getTableName();

                        //获取并解析StoreValue
                        CanalEntry.RowChange rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        //行数据集
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //INSERT,UPDATE,CREATE,ALTER
                        CanalEntry.EventType eventType = rowChange.getEventType();

                        //处理数据并发送至kafka
                        handler(tableName, rowDatasList, eventType);


                    }
                }
            }
        }
    }


    private static void handler(String tableName, List<CanalEntry.RowData> rowDatasList, CanalEntry.EventType eventType) {

        //订单表并且是下单数据
        if ("order_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)) {

            for (CanalEntry.RowData rowData : rowDatasList) {

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }

                //测试打印
                System.out.println(jsonObject.toString());

                //发送至kafka
                KafkaSender.send(GmallConstants.GMALL_ORDER_TOPIC, jsonObject.toString());
            }

        }else if ("order_detail".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType)){

            for (CanalEntry.RowData rowData : rowDatasList) {

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }

                //测试打印
                System.out.println(jsonObject.toString());

                //发送至kafka
                KafkaSender.send(GmallConstants.GMALL_ORDER_DETAIL_TOPIC, jsonObject.toString());
            }



        }else if ("user_info".equals(tableName) && CanalEntry.EventType.INSERT.equals(eventType) || CanalEntry.EventType.UPDATE.equals(eventType)){

            for (CanalEntry.RowData rowData : rowDatasList) {

                JSONObject jsonObject = new JSONObject();

                for (CanalEntry.Column column : rowData.getAfterColumnsList()) {
                    jsonObject.put(column.getName(), column.getValue());
                }

                //测试打印
                System.out.println(jsonObject.toString());

                //发送至kafka
                KafkaSender.send(GmallConstants.GMALL_USER_TOPIC, jsonObject.toString());
            }
        }

    }
}
