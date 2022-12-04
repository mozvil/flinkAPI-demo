package com.mozvil.time;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EventBean implements Serializable {

	private static final long serialVersionUID = -5621284441883579187L;

	private Long guid;
	private String eventId;
	private Long timestamp;
	private String pageId;

}
