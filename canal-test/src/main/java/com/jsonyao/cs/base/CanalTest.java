package com.jsonyao.cs.base;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;

import java.net.InetSocketAddress;
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

    }
}
