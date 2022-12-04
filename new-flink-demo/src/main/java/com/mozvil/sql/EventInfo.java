package com.mozvil.sql;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class EventInfo implements Serializable {

	private static final long serialVersionUID = 2798880410108900116L;
	
	public int guid;
	public String eventId;
	public long eventTime;
	public String pageId;

}
