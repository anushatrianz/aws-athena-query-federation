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
import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.sql.SQLException;

/**
 * Boilerplate composite handler that allows us to use a single Lambda function for both
 * Metadata and Data. In this case we just compose {@link PostGreSqlMetadataHandler} and {@link PostGreSqlRecordHandler}.
 *
 * Recommend using {@link PostGreSqlMuxCompositeHandler} instead.
 */
public class PostGreSqlCompositeHandler
        extends CompositeHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(PostGreSqlCompositeHandler.class);

    public PostGreSqlCompositeHandler()
    {
        super(new PostGreSqlMetadataHandler(new PostGreSqlEnvironmentProperties().createEnvironment()), new PostGreSqlRecordHandler(new PostGreSqlEnvironmentProperties().createEnvironment()));
    }

    @Override
    public Exception handleException(Exception e)
    {
        return handlePostGreSqlException(super.handleException(e));
    }

    /**
     * Transforms a PostgreSQL {@link SQLException} into an {@link AthenaConnectorException} using the SQLSTATE
     * reported by the server. Returns other exceptions unchanged.
     *
     * @param e The exception to handle
     * @return The transformed exception or original exception
     */
    public static Exception handlePostGreSqlException(Exception e)
    {
        if (e instanceof AthenaConnectorException) {
            return e;
        }
        SQLException sqlException = findCause(e, SQLException.class);
        if (sqlException == null) {
            return e;
        }
        String message = sqlException.getMessage() != null ? sqlException.getMessage() : sqlException.getClass().getSimpleName();
        FederationSourceErrorCode errorCode = getErrorCode(sqlException);
        return new AthenaConnectorException(message, sqlException, ErrorDetails.builder()
                .errorCode(errorCode.toString())
                .errorMessage(message)
                .build());
    }

    private static FederationSourceErrorCode getErrorCode(SQLException e)
    {
        if (findCause(e, UnknownHostException.class) != null) {
            return FederationSourceErrorCode.INVALID_INPUT_EXCEPTION;
        }
        if (findCause(e, SocketTimeoutException.class) != null) {
            return FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION;
        }

        String sqlState = e.getSQLState();
        if (sqlState == null || sqlState.length() != 5) {
            return FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION;
        }

        switch (sqlState) {
            case "28000": // invalid_authorization_specification
            case "28P01": // invalid_password
                return FederationSourceErrorCode.INVALID_CREDENTIALS_EXCEPTION;
            case "42501": // insufficient_privilege
                return FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION;
            case "3D000": // invalid_catalog_name (database does not exist)
            case "3F000": // invalid_schema_name
            case "42P01": // undefined_table
            case "42703": // undefined_column
            case "42704": // undefined_object
                return FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION;
            case "0A000": // feature_not_supported
                return FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION;
            case "57014": // query_canceled (statement_timeout)
                return FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION;
            case "53300": // too_many_connections
                return FederationSourceErrorCode.THROTTLING_EXCEPTION;
            default:
                break;
        }

        switch (sqlState.substring(0, 2)) {
        case "22": // data_exception
        case "42": // syntax_error_or_access_rule_violation
            return FederationSourceErrorCode.INVALID_INPUT_EXCEPTION;
        default:
            return FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION;
    }
    }

    private static <T extends Throwable> T findCause(Throwable throwable, Class<T> type)
    {
        Throwable current = throwable;
        while (current != null) {
            if (type.isInstance(current)) {
                return type.cast(current);
            }
            if (current.getCause() == current) {
                break;
            }
            current = current.getCause();
        }
        return null;
    }
}
