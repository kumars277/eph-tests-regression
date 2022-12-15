package com.eph.automation.testing.models.api;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class AccessToken
{
	private String accessToken;
	public String getaccessToken(){return accessToken;}

	private String refreshToken;
	public String getrefreshToken(){return refreshToken;}

	private String scope;
	public String getscope(){return scope;}

	private String tokenType;
	public String gettokenType(){return tokenType;}

	private Long expiresin;
	public Long getexpiresin(){return expiresin;}

	/*private Long expiresIn;
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
	public String getToken(){return token;}*/

	@JsonCreator
	/*public AccessToken(@JsonProperty(value = "token_type", required = true) String tokenType,
			           @JsonProperty(value = "expires_in", required = true) Long expiresIn,
			           @JsonProperty(value = "ext_expires_in", required = true) Long extExpiresIn,
			           @JsonProperty(value = "expires_on", required = true) Long expiresOn,
			           @JsonProperty(value = "not_before", required = true) Long notBefore,
			           @JsonProperty(value = "resource", required = true) String resource,
			           @JsonProperty(value = "access_token", required = true) String token)*/
	public AccessToken(@JsonProperty(value = "access_token", required = true) String accessToken,
					   @JsonProperty(value = "refresh_token", required = true) String refreshToken,
					   @JsonProperty(value = "scope", required = true) String scope,
					   @JsonProperty(value = "token_type", required = true) String tokenType,
					   @JsonProperty(value = "expires_in", required = true) Long expiresin)

	{
		/*this.expiresIn = expiresIn;
		this.extExpiresIn = extExpiresIn;
		this.expiresOn = expiresOn;
		this.notBefore = notBefore;
		this.resource = resource;
		this.token = token;*/
		this.accessToken =accessToken;
		this.refreshToken = refreshToken;
		this.scope= scope;
		this.expiresin = expiresin;
		this.tokenType = tokenType;
	}

	public boolean isValid(Long expiryOffsetSeconds)
	{
		Long nowInSeconds = (new Date().getTime()) / 1000;

		boolean expiryValid = nowInSeconds <= (expiresin - expiryOffsetSeconds);
		//boolean notBeforeValid = nowInSeconds >= notBefore;
		//return expiryValid && notBeforeValid;
		return expiryValid;
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
		return "AccessToken [accessToken=" + accessToken + ", refreshToken=" + refreshToken + ", scope=" + scope
				+ ", expiresin=" + toDate(expiresin) + ", tokenType="
				+ tokenType + "]";
	}



}