/*-
 * #%L
 * athena-postgresql
 * %%
 * Copyright (C) 2019 Amazon Web Services
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
package com.amazonaws.athena.connectors.postgresql;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import org.junit.Test;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class PostGreSqlCompositeHandlerTest
{
    @Test
    public void handleException_withInvalidPassword_returnsInvalidCredentials()
    {
        assertErrorCode("28P01", FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION);
        assertErrorCode("28000", FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION);
    }

    @Test
    public void handleException_withInsufficientPrivilege_returnsAccessDenied()
    {
        assertErrorCode("42501", FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION);
    }

    @Test
    public void handleException_withUndefinedObject_returnsEntityNotFound()
    {
        assertErrorCode("3D000", FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION);
        assertErrorCode("3F000", FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION);
        assertErrorCode("42P01", FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION);
        assertErrorCode("42703", FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION);
        assertErrorCode("42704", FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION);
    }

    @Test
    public void handleException_withFeatureNotSupported_returnsOperationNotSupported()
    {
        assertErrorCode("0A000", FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION);
    }

    @Test
    public void handleException_withQueryCanceled_returnsOperationTimeout()
    {
        assertErrorCode("57014", FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION);
    }

    @Test
    public void handleException_withTooManyConnections_returnsThrottling()
    {
        assertErrorCode("53300", FederationSourceErrorCode.THROTTLING_EXCEPTION);
    }

    @Test
    public void handleException_withSyntaxOrDataError_returnsInvalidInput()
    {
        assertErrorCode("42601", FederationSourceErrorCode.INVALID_INPUT_EXCEPTION);
        assertErrorCode("42883", FederationSourceErrorCode.INVALID_INPUT_EXCEPTION);
        assertErrorCode("22P02", FederationSourceErrorCode.INVALID_INPUT_EXCEPTION);
    }

    @Test
    public void handleException_withConnectionOrUnknownState_returnsInternalService()
    {
        assertErrorCode("08006", FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION);
        assertErrorCode("XX000", FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION);
        assertErrorCode(null, FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION);
    }

    @Test
    public void handleException_withMalformedSqlState_returnsInternalService()
    {
        // A SQLSTATE that is not 5 characters long must not be matched by its class prefix ("42" would be InvalidInput).
        assertErrorCode("42", FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION);
        assertErrorCode("", FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION);
    }

    @Test
    public void handleException_withUnknownHostCause_returnsInvalidInput()
    {
        SQLException sqlException = new SQLException("The connection attempt failed.", "08001", new UnknownHostException("bad-host"));
        assertEquals(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString(), errorCodeOf(PostGreSqlCompositeHandler.handlePostGreSqlException(sqlException)));
    }

    @Test
    public void handleException_withSocketTimeoutCause_returnsOperationTimeout()
    {
        SQLException sqlException = new SQLException("An I/O error occurred while sending to the backend.", "08006", new SocketTimeoutException("Read timed out"));
        assertEquals(FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION.toString(), errorCodeOf(PostGreSqlCompositeHandler.handlePostGreSqlException(sqlException)));
    }

    @Test
    public void handleException_withWrappedSqlException_mapsAndKeepsCause()
    {
        SQLException sqlException = new SQLException("permission denied for table orders", "42501");
        Exception result = PostGreSqlCompositeHandler.handlePostGreSqlException(new RuntimeException(sqlException));

        assertTrue(result instanceof AthenaConnectorException);
        assertEquals(FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION.toString(), errorCodeOf(result));
        // getMessage() appends the error code, which is what Athena shows to the user.
        assertEquals("permission denied for table orders [" + FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION + "]", result.getMessage());
        assertEquals("permission denied for table orders", ((AthenaConnectorException) result).getErrorDetails().errorMessage());
        assertSame(sqlException, result.getCause());
    }

    @Test
    public void handleException_withAthenaConnectorException_returnsSameInstance()
    {
        AthenaConnectorException original = new AthenaConnectorException("already classified",
                ErrorDetails.builder().errorCode(FederationSourceErrorCode.INVALID_INPUT_EXCEPTION.toString()).build());
        assertSame(original, PostGreSqlCompositeHandler.handlePostGreSqlException(original));
    }

    @Test
    public void handleException_withNonSqlException_returnsSameInstance()
    {
        IllegalStateException original = new IllegalStateException("not from the database");
        assertSame(original, PostGreSqlCompositeHandler.handlePostGreSqlException(original));
    }

    private static void assertErrorCode(String sqlState, FederationSourceErrorCode expected)
    {
        Exception result = PostGreSqlCompositeHandler.handlePostGreSqlException(new SQLException("error", sqlState));
        assertEquals("SQLSTATE " + sqlState, expected.toString(), errorCodeOf(result));
    }

    private static String errorCodeOf(Exception e)
    {
        assertTrue(e instanceof AthenaConnectorException);
        return ((AthenaConnectorException) e).getErrorDetails().errorCode();
    }
}
