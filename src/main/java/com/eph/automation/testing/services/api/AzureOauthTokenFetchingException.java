package com.eph.automation.testing.services.api;

public class AzureOauthTokenFetchingException extends Exception
{
	private static final long serialVersionUID = -5019384776601593698L;

	public AzureOauthTokenFetchingException(String message, Throwable cause)
	{
		super(message, cause);
	}

	public AzureOauthTokenFetchingException(String message)
	{
		super(message);
	}

}
