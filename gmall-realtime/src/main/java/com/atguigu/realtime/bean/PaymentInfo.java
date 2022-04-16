package com.atguigu.realtime.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;

/**
 * @author sunsicheng
 * @version 1.0
 * @date 2022/4/16 10:53
 */
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
}
