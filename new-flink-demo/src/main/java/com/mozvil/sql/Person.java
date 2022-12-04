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
public class Person implements Serializable {

	private static final long serialVersionUID = -1926652405551157127L;
	public Integer id;
	public String name;
	public Integer age;
	public String gender;

}
