package net.acan.realtime.service;

import net.acan.realtime.mapper.ProductMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
public class ProductServiceImpl implements ProductService{
    @Autowired(required = false)
    ProductMapper productMapper;
    @Override
    public BigDecimal gmv(int date) {
        return productMapper.gmv(date);
    }


    @Override
    public List<Map<String, Object>> gmvByTm(int date) {

        System.out.println(productMapper.gmvByTm(date));
        return productMapper.gmvByTm(date);
    }
}
