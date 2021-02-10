package com.jsonyao.cs.base;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;

/**
 * Canal测试类
 */
public class CanalTest {

    /**
     * 测试步骤:
     *  1. 连接Canal服务器
     * 	2. 向Master请求dump协议
     * 	3. 把发送过来的binlog进行解析
     * 	4. 最后做实际的处境处理...发送到MQ Print...
     * @param args
     */
    public static void main(String[] args) {
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("192.168.1.113", 11111), "example", "root", "root");

        int curEmptyCount = 0;// 当前拉空次数
        int batchSize = 1000;// 一次拉取的数量
        try {
            // 连接Canal服务器
            canalConnector.connect();
            // 订阅库表信息: 订阅全部库表
            canalConnector.subscribe(".*\\..*");
            // 如果出现问题, 则直接回滚
            canalConnector.rollback();

            int maxEmptyCount = 1200;// 最大拉空次数
            while (curEmptyCount < maxEmptyCount) {
                // 获取指定数量的拉取数据
                Message message = canalConnector.getWithoutAck(batchSize);

                // 数据处理
                long batchId = message.getId();
                int size = message.getEntries().size();
                if(batchId == -1 || size == 0){
                    // 累计拉空次数
                    System.err.println("empty count: " + ++curEmptyCount);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        // ignore..
                    }
                }else {
                    // 清空拉空次数
                    curEmptyCount = 0;
                    System.err.printf("message[batchId=%s, size=%s] \n", batchId, size);

                    // 处理不为空的数据
                    dealEntries(message.getEntries());
                }

                // 确认提交处理后的数据
                canalConnector.ack(batchId);
            }

            // 当前拉空次数大于最大拉空次数, 则退出拉取
            System.err.println("empty too many times, exit");
        }finally {
            // 关闭连接
            canalConnector.disconnect();
        }
    }

    /**
     * 处理不为空的数据
     * @param entries
     */
    private static void dealEntries(List<CanalEntry.Entry> entries) {
        for (CanalEntry.Entry entry : entries) {
            // 如果EntryType处于事务中, 则不做任何处理
            if(CanalEntry.EntryType.TRANSACTIONBEGIN == entry.getEntryType() || CanalEntry.EntryType.TRANSACTIONEND == entry.getEntryType()){
                continue;
            }

            // 否则转换二进制数据为RowChange对象
            CanalEntry.RowChange rowChange = null;
            try {
                rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
            } catch (InvalidProtocolBufferException e) {
                e.printStackTrace();
            }

            // 获取EventType
            CanalEntry.EventType eventType = rowChange.getEventType();

            // 查看内容
            CanalEntry.Header header = entry.getHeader();
            System.err.println(String.format("binlog[%s:%s], name[%s,%s], eventType : %s",
                    header.getLogfileName(), header.getLogfileOffset(), header.getSchemaName(), header.getTableName(), eventType));

            // 真正执行数据处理
            List<CanalEntry.Column> beforeColumnsList = new ArrayList<>();
            List<CanalEntry.Column> afterColumnsList = new ArrayList<>();
            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                // 删除数据
                if(eventType == CanalEntry.EventType.DELETE){
                    beforeColumnsList = rowData.getBeforeColumnsList();
                    afterColumnsList = rowData.getAfterColumnsList();// 删除时为空列表

                    // 处理变化数据
                    printColumn(beforeColumnsList);
                    printColumn(afterColumnsList);
                }
                // 插入数据
                else if(eventType == CanalEntry.EventType.INSERT){
                    beforeColumnsList = rowData.getBeforeColumnsList();// 插入时为空列表
                    afterColumnsList = rowData.getAfterColumnsList();

                    // 处理变化数据
                    printColumn(beforeColumnsList);
                    printColumn(afterColumnsList);
                }
                // 更新数据
                else {
                    beforeColumnsList = rowData.getBeforeColumnsList();
                    afterColumnsList = rowData.getAfterColumnsList();

                    // 处理变化数据
                    printColumn(beforeColumnsList);
                    printColumn(afterColumnsList);
                }
            }
        }
    }

    /**
     * 处理变化数据
     * @param columnsList
     */
    private static void printColumn(List<CanalEntry.Column> columnsList) {
        for (CanalEntry.Column column : columnsList) {
            System.err.println(column.getName() + ":" + column.getValue() + ", update=" + column.getUpdated() + "...");
        }
    }
}
