package net.acan.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class Province {
    private String province_name;
    private BigDecimal order_amount;
    private Long order_count;
}
