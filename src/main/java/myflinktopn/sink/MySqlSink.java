package myflinktopn.sink;

import myflinktopn.TopNJob;
import myflinktopn.db.DbUtils;
import myflinktopn.pojo.UserAction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.List;

/**
 * @author huangqingshi
 * @Date 2019-12-07
 */
public class MySqlSink extends RichSinkFunction<List<TopNJob.ItemBuyCount>> {

    private PreparedStatement ps;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //获取数据库连接，准备写入数据库
        connection = DbUtils.getConnection();
        String sql = "insert into itembuycount(itemId, buyCount, createDate) values (?, ?, ?); ";
        ps = connection.prepareStatement(sql);
        System.out.println("-------------open------------");
    }

    @Override
    public void close() throws Exception {
        super.close();
        //关闭并释放资源
        if(connection != null) {
            connection.close();
        }

        if(ps != null) {
            ps.close();
        }
        System.out.println("-------------close------------");
    }

    @Override
    public void invoke(List<TopNJob.ItemBuyCount> topNItems, Context context) throws Exception {
        for(TopNJob.ItemBuyCount itemBuyCount : topNItems) {
            ps.setLong(1, itemBuyCount.itemId);
            ps.setLong(2, itemBuyCount.buyCount);
            ps.setTimestamp(3, new Timestamp(itemBuyCount.windowEnd));
            ps.addBatch();
        }

        //一次性写入
        int[] count = ps.executeBatch();
        System.out.println("-------------invoke------------");
        System.out.println("成功写入Mysql数量：" + count.length);

    }
}
