package net.acan.gmall.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.text.ParseException;
import java.text.SimpleDateFormat;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PaymentInfo {
    private Long id;
    private Long order_id;
    private Long user_id;
    private BigDecimal total_amount;
    private String subject;
    private String payment_type;
    private String create_time;
    private String callback_time;
    private Long ts;
    
    
    public void setCreate_time(String create_time) throws ParseException {
        this.create_time = create_time;
        
        this.ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(create_time).getTime();
        
    }
}
