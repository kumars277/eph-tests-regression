package com.eph.automation.testing.models.api;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AccessToken
{
	private String tokenType;
	public String getTokenType(){return tokenType;}

	private Long expiresIn;
	public Long getExpiresIn(){return expiresIn;}

	private Long extExpiresIn;
	public Long getExtExpiresIn(){return extExpiresIn;}

	private Long expiresOn;
	public Long getExpiresOn(){return expiresOn;}
	public String getExpiresOnAsString(){return toDate(expiresOn);}

	private Long notBefore;
	public Long getNotBefore(){return notBefore;}
	public String getNotBeforeAsString(){return toDate(notBefore);}

	private String resource;
	public String getResource(){return resource;}

	private String token;
	public String getToken(){return token;}
	
	@JsonCreator
	public AccessToken(@JsonProperty(value = "token_type", required = true) String tokenType, 
			           @JsonProperty(value = "expires_in", required = true) Long expiresIn, 
			           @JsonProperty(value = "ext_expires_in", required = true) Long extExpiresIn, 
			           @JsonProperty(value = "expires_on", required = true) Long expiresOn, 
			           @JsonProperty(value = "not_before", required = true) Long notBefore,
			           @JsonProperty(value = "resource", required = true) String resource, 
			           @JsonProperty(value = "access_token", required = true) String token)
	{
		this.tokenType = tokenType;
		this.expiresIn = expiresIn;
		this.extExpiresIn = extExpiresIn;
		this.expiresOn = expiresOn;
		this.notBefore = notBefore;
		this.resource = resource;
		this.token = token;
	}
	


	public boolean isValid(Long expiryOffsetSeconds)
	{
		Long nowInSeconds = (new Date().getTime()) / 1000;
		
		boolean expiryValid = nowInSeconds <= (expiresOn - expiryOffsetSeconds);
		boolean notBeforeValid = nowInSeconds >= notBefore;
		
		return expiryValid && notBeforeValid;		
	}
	
	private static String toDate(Long timeSeconds)
	{
		if (timeSeconds == null)
		{
			return null;
		}
		else
		{
			Date date = new Date(timeSeconds * 1000);
			DateFormat formatter = new SimpleDateFormat("yyyy.MM.dd HH:mm:ss");
			return formatter.format(date);
		}
			
	}
	
	@Override
	public String toString()
	{
		return "AccessToken [tokenType=" + tokenType + ", expiresIn=" + expiresIn + ", extExpiresIn=" + extExpiresIn
				+ ", expiresOn=" + toDate(expiresOn) + ", notBefore=" + toDate(notBefore) + ", resource=" + resource + ", accessToken="
				+ token + "]";
	}
	
	
	
}
