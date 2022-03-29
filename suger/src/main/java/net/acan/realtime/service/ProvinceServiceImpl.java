package net.acan.realtime.service;

import net.acan.realtime.bean.Province;
import net.acan.realtime.mapper.ProductMapper;
import net.acan.realtime.mapper.ProvinceMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Service
public class ProvinceServiceImpl implements ProvinceService{
    @Autowired(required = false)
    ProvinceMapper provinceMapper;

    @Override
    public List<Province> statProvince(int date) {
        return provinceMapper.statProvince(date);
    }
}
