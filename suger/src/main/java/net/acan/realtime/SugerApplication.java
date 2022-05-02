package net.acan.realtime;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "net.acan.realtime.mapper")
public class SugerApplication {

    public static void main(String[] args) {
        SpringApplication.run(SugerApplication.class, args);
    }

}
