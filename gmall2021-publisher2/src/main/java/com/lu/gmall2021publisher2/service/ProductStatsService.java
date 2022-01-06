package com.lu.gmall2021publisher2.service;

import java.math.BigDecimal;
import java.util.Map;

public interface ProductStatsService {

    BigDecimal getGmv(int date);

    Map getGmvByTm(int date, int limit);
}
