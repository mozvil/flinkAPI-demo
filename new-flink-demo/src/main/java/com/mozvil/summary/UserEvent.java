package com.mozvil.summary;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class UserEvent implements Serializable {

	private static final long serialVersionUID = 1948415158425539221L;
	
	private Integer id;
	private String gender;
	private String city;
	private String eventName;
	private Integer eventCount;

}
