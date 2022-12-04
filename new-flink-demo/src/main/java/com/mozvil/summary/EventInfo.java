package com.mozvil.summary;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.ToString;

@Data
@AllArgsConstructor
@ToString
public class EventInfo implements Serializable {
	
	private static final long serialVersionUID = -4754027711191765385L;
	
	private Integer id;
	private String eventName;
	private Integer eventCount;

}
