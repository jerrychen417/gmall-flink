package com.lu.gmall2021publisher2;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@MapperScan(basePackages = "com.lu.gmall2021publisher2.mapper")
public class Gmall2021Publisher2Application {

    public static void main(String[] args) {

        SpringApplication.run(Gmall2021Publisher2Application.class, args);
    }

}
