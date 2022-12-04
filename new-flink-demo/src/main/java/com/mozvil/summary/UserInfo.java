package com.mozvil.summary;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class UserInfo implements Serializable {

	private static final long serialVersionUID = -4702422119718110345L;
	
	private Integer id;
	private String gender;
	private String city;

}
