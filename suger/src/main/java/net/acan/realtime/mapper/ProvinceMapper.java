package net.acan.realtime.mapper;

import net.acan.realtime.bean.Province;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public interface ProvinceMapper {

    @Select("select province_name,sum(order_amount) as order_amount,sum(order_count) as order_count\n" +
            "from province_stats_2022\n" +
            "where toYYYYMMDD(stt) = #{date}\n" +
            "group by province_name")
    List<Province> statProvince(@Param("date") int date);


}
