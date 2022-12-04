package com.mozvil.demo;

import java.io.Serializable;
import java.util.Map;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@NoArgsConstructor
@AllArgsConstructor
@Getter
@Setter
@ToString
public class Event implements Serializable {

	private static final long serialVersionUID = 3208142274702365764L;
	
	private String user;
	private String eventId;
	private Long timestamp;
    private Map<String, String> eventInfo;

}
