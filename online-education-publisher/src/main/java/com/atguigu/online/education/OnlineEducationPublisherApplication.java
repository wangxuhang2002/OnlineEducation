package com.atguigu.online.education;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.atguigu.gmall.mapper")
public class OnlineEducationPublisherApplication {

    public static void main(String[] args) {
        SpringApplication.run(OnlineEducationPublisherApplication.class, args);
    }
}
