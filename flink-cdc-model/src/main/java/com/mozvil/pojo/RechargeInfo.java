package com.mozvil.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RechargeInfo implements Serializable {

    private static final long serialVersionUID = 1L;

    private UserInfo userInfo;
    private Integer price;
    private Date actionTime;
    private Integer payMethod;
    private String remark;
}
