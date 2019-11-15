package com.eph.automation.testing.models.api;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AccessToken
{
	private String tokenType;
	
	private Long expiresIn;
	
	private Long extExpiresIn;

	private Long expiresOn;
	
	private Long notBefore;
	
	private String resource;
	
	private String token;
	
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
	
	public String getTokenType()
	{
		return tokenType;
	}

	public Long getExpiresIn()
	{
		return expiresIn;
	}

	public Long getExtExpiresIn()
	{
		return extExpiresIn;
	}

	public Long getExpiresOn()
	{
		return expiresOn;
	}

	public String getExpiresOnAsString()
	{
		return toDate(expiresOn);
	}

	public Long getNotBefore()
	{
		return notBefore;
	}

	public String getNotBeforeAsString()
	{
		return toDate(notBefore);
	}

	public String getResource()
	{
		return resource;
	}

	public String getToken()
	{
		return token;
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
