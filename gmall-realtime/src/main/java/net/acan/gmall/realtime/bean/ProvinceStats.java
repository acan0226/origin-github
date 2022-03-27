package net.acan.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProvinceStats {
    
    private Long province_id;
    private String province_name;
    private String area_code;
    private String iso_code;
    private String iso_3166_2;
//    private Timestamp stt;
//    private Timestamp edt;
    private String stt;
    private String edt;
    private BigDecimal order_amount;
    private Long order_count;
    private Long ts;
}
