/*-
 * #%L
 * athena-google-bigquery
 * %%
 * Copyright (C) 2019 - 2022 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQuerySQLException;
import com.google.cloud.bigquery.JobException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import software.amazon.awssdk.services.secretsmanager.SecretsManagerClient;
import software.amazon.awssdk.services.secretsmanager.model.GetSecretValueResponse;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;


@RunWith(MockitoJUnitRunner.class)
public class BigQueryCompositeHandlerTest
{
    static {
        System.setProperty("aws.region", "us-east-1");
    }

    MockedStatic<SecretsManagerClient> awsSecretManagerClient;
    MockedStatic<ServiceAccountCredentials> serviceAccountCredentialsStatic;
    MockedStatic<BigQueryUtils> bigQueryUtils;
    private BigQueryCompositeHandler bigQueryCompositeHandler;
    @Mock
    private SecretsManagerClient secretsManager;
    @Mock
    private ServiceAccountCredentials serviceAccountCredentials;

    @Before
    public void setUp()
    {
        bigQueryUtils = mockStatic(BigQueryUtils.class);
        serviceAccountCredentialsStatic = mockStatic(ServiceAccountCredentials.class);
        awsSecretManagerClient = mockStatic(SecretsManagerClient.class);
    }

    @After
    public void cleanup()
    {
        awsSecretManagerClient.close();
        serviceAccountCredentialsStatic.close();
        bigQueryUtils.close();
    }

    @Test
    public void bigQueryCompositeHandlerTest() throws IOException
    {
        Exception ex = null;

        Mockito.when(SecretsManagerClient.create()).thenReturn(secretsManager);
        GetSecretValueResponse getSecretValueResponse = GetSecretValueResponse.builder()
                .versionStages(Arrays.asList("v1"))
                .secretString("{\n" +
                        "  \"type\": \"service_account\",\n" +
                        "  \"project_id\": \"mockProjectId\",\n" +
                        "  \"private_key_id\": \"mockPrivateKeyId\",\n" +
                        "  \"private_key\": \"-----BEGIN PRIVATE KEY-----\\nmockPrivateKeydsfhdskfhjdfjkdhgfdjkghfdngvfkvfnjvfdjkg\\n-----END PRIVATE KEY-----\\n\",\n" +
                        "  \"client_email\": \"mockabc@mockprojectid.iam.gserviceaccount.com\",\n" +
                        "  \"client_id\": \"000000000000000000000\"\n" +
                        "}")
                .build();

        Mockito.when(ServiceAccountCredentials.fromStream(any())).thenReturn(serviceAccountCredentials);
        bigQueryCompositeHandler = new BigQueryCompositeHandler();
        assertTrue(bigQueryCompositeHandler instanceof BigQueryCompositeHandler);
    }

    /**
     * BigQueryException with various HTTP error codes
     */
    @Test
    public void testBigQueryExceptionHttpCodeMapping() {
        BigQueryException exception = new BigQueryException(400, "Invalid query");
        Exception result = BigQueryCompositeHandler.handleBigQueryException(exception);

        assertTrue(result instanceof AthenaConnectorException);
        AthenaConnectorException athenaException = (AthenaConnectorException) result;
        assertEquals(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString(),
                athenaException.getErrorDetails().errorCode());
        // getMessage() can append the error code, so compare the start of the message and the cause.
        assertTrue(athenaException.getMessage().startsWith("Invalid query"));
        assertSame(exception, athenaException.getCause());
    }

    // JobException Tests
    @Test
    public void testHandleJobException_NoHttpCode()
    {
        JobException exception = mock(JobException.class);
        Exception result = BigQueryCompositeHandler.handleBigQueryException(exception);

        assertTrue(result instanceof AthenaConnectorException);
        AthenaConnectorException athenaException = (AthenaConnectorException) result;
        assertEquals(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString(),
                athenaException.getErrorDetails().errorCode());
    }

    // BigQuerySQLException Tests
    @Test
    public void testHandleBigQuerySQLException()
    {
        BigQuerySQLException exception = new BigQuerySQLException("SQL syntax error");
        Exception result = BigQueryCompositeHandler.handleBigQueryException(exception);

        assertTrue(result instanceof AthenaConnectorException);
        AthenaConnectorException athenaException = (AthenaConnectorException) result;
        assertEquals(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString(),
                athenaException.getErrorDetails().errorCode());
        assertTrue(athenaException.getMessage().startsWith("SQL syntax error"));
    }

    // Non-BigQuery Exception Tests
    @Test
    public void testHandleNonBigQueryException_RuntimeException()
    {
        RuntimeException exception = new RuntimeException("Some other error");
        Exception result = BigQueryCompositeHandler.handleBigQueryException(exception);
        assertSame(exception, result);
    }

    @Test
    public void testHandleNonBigQueryException_SQLException()
    {
        SQLException exception = new SQLException("Generic SQL error");
        Exception result = BigQueryCompositeHandler.handleBigQueryException(exception);
        assertSame(exception, result);
    }

    @Test
    public void testHandleBigQueryException_NullMessage()
    {
        BigQueryException exception = new BigQueryException(403, null);
        Exception result = BigQueryCompositeHandler.handleBigQueryException(exception);

        assertTrue(result instanceof AthenaConnectorException);
        AthenaConnectorException athenaException = (AthenaConnectorException) result;
        assertEquals(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION.toString(),
                athenaException.getErrorDetails().errorCode());
    }

    // BigQuery error reason tests
    @Test
    public void handleBigQueryException_quotaReasonWithHttp403_returnsThrottling()
    {
        // Quota and rate limit errors use HTTP 403, which would otherwise be reported as AccessDenied.
        BigQueryException exception = new BigQueryException(403, "Exceeded rate limits",
                new BigQueryError("rateLimitExceeded", "global", "Exceeded rate limits"));

        assertErrorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(exception));
    }

    @Test
    public void handleBigQueryException_unknownReason_fallsBackToHttpCode()
    {
        BigQueryException exception = new BigQueryException(404, "Not found: Table",
                new BigQueryError("someNewReason", "global", "Not found: Table"));

        assertErrorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(exception));
    }

    @Test
    public void handleBigQueryException_jobExceptionWithKnownReasons_mapsReasonToErrorCode()
    {
        assertErrorCode(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(jobException("accessDenied")));
        assertErrorCode(FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(jobException("notFound")));
        assertErrorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(jobException("invalidQuery")));
        assertErrorCode(FederationSourceErrorCode.THROTTLING_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(jobException("quotaExceeded")));
        assertErrorCode(FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(jobException("backendError")));
        assertErrorCode(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(jobException("timeout")));
    }

    @Test
    public void handleBigQueryException_jobExceptionWithUnknownReason_returnsInvalidInput()
    {
        assertErrorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(jobException("someNewReason")));
    }

    @Test
    public void handleBigQueryException_sqlExceptionWithAccessDeniedReason_returnsAccessDenied()
    {
        BigQuerySQLException exception = new BigQuerySQLException("denied",
                Arrays.asList(new BigQueryError("accessDenied", "global", "denied")));

        assertErrorCode(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION, BigQueryCompositeHandler.handleBigQueryException(exception));
    }

    // BigQuery Storage Read API (gRPC) tests
    @Test
    public void handleBigQueryException_apiExceptionWithEachStatusCode_mapsToExpectedErrorCode()
    {
        Map<StatusCode.Code, FederationSourceErrorCode> expected = new HashMap<>();
        expected.put(StatusCode.Code.PERMISSION_DENIED, FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION);
        expected.put(StatusCode.Code.UNAUTHENTICATED, FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION);
        expected.put(StatusCode.Code.NOT_FOUND, FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION);
        expected.put(StatusCode.Code.INVALID_ARGUMENT, FederationSourceErrorCode.INVALID_INPUT_EXCEPTION);
        expected.put(StatusCode.Code.RESOURCE_EXHAUSTED, FederationSourceErrorCode.THROTTLING_EXCEPTION);
        expected.put(StatusCode.Code.DEADLINE_EXCEEDED, FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION);
        expected.put(StatusCode.Code.UNIMPLEMENTED, FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION);
        expected.put(StatusCode.Code.INTERNAL, FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION);
        expected.put(StatusCode.Code.UNAVAILABLE, FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION);

        for (Map.Entry<StatusCode.Code, FederationSourceErrorCode> entry : expected.entrySet()) {
            ApiException exception = apiException(entry.getKey());
            Exception result = BigQueryCompositeHandler.handleBigQueryException(exception);
            assertErrorCode(entry.getValue(), result);
            assertSame(exception, result.getCause());
        }
    }

    @Test
    public void handleBigQueryException_athenaConnectorException_returnsSameInstance()
    {
        AthenaConnectorException exception = new AthenaConnectorException("already mapped",
                ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());

        assertSame(exception, BigQueryCompositeHandler.handleBigQueryException(exception));
    }

    @Test
    public void handleBigQueryException_storageApiPermissionDenied_returnsAccessDenied()
    {
        // The Storage Read API throws ApiException directly, so only the top level exception needs checking.
        ApiException exception = apiException(StatusCode.Code.PERMISSION_DENIED);
        Exception result = BigQueryCompositeHandler.handleBigQueryException(exception);

        assertErrorCode(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION, result);
        assertSame(exception, result.getCause());
    }

    @Test
    public void handleException_bigQueryException_returnsAthenaConnectorException() throws IOException
    {
        // handleException is the entry point used by CompositeHandler.handleRequest.
        Mockito.when(SecretsManagerClient.create()).thenReturn(secretsManager);
        Mockito.when(ServiceAccountCredentials.fromStream(any())).thenReturn(serviceAccountCredentials);
        bigQueryCompositeHandler = new BigQueryCompositeHandler();

        Exception result = bigQueryCompositeHandler.handleException(new BigQueryException(403, "Access Denied: Table x"));

        assertErrorCode(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION, result);
    }

    private static void assertErrorCode(FederationSourceErrorCode expected, Exception result)
    {
        assertTrue(result instanceof AthenaConnectorException);
        assertEquals(expected.toString(), ((AthenaConnectorException) result).getErrorDetails().errorCode());
    }

    private static JobException jobException(String reason)
    {
        JobException exception = mock(JobException.class);
        Mockito.when(exception.getErrors()).thenReturn(Arrays.asList(new BigQueryError(reason, "global", "job failed")));
        return exception;
    }

    private static ApiException apiException(StatusCode.Code code)
    {
        StatusCode statusCode = new StatusCode()
        {
            @Override
            public Code getCode()
            {
                return code;
            }

            @Override
            public Object getTransportCode()
            {
                return code;
            }
        };
        return new ApiException("Storage API failure " + code, new RuntimeException("transport"), statusCode, false);
    }
}
