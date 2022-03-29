package net.acan.realtime.mapper;

import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ProductMapper {
    //1.gmv
    @Select("select sum(order_amount) as order_amount from product_stats_2022 where toYYYYMMDD(stt) = #{date}")
    BigDecimal gmv(@Param("date") int date);

    //2.gmvbytm
    @Select("select tm_name,sum(order_amount) as order_amount\n" +
            "from product_stats_2022\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by tm_name")
    List<Map<String,Object>> gmvByTm(@Param("date") int date);


}
