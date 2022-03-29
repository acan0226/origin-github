package net.acan.gmall.realtime.Function;

import net.acan.gmall.realtime.bean.SourceCt;
import org.apache.flink.table.functions.TableFunction;

public class KwProduct extends TableFunction<SourceCt> {
    public void eval(Long click_ct, Long cart_ct, Long order_ct){
        if (click_ct > 0) {
            collect(new SourceCt("click_ct",click_ct));
        }

        if (cart_ct > 0) {
            collect(new SourceCt("cart_ct",cart_ct));
        }

        if (order_ct > 0) {
            collect(new SourceCt("order_ct",order_ct));
        }

    }
}
