
/*-
 * #%L
 * athena-google-bigquery
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
package com.amazonaws.athena.connectors.google.bigquery;

import com.amazonaws.athena.connector.lambda.exceptions.AthenaConnectorException;
import com.amazonaws.athena.connector.lambda.handlers.CompositeHandler;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQuerySQLException;
import com.google.cloud.bigquery.JobException;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.glue.model.ErrorDetails;
import software.amazon.awssdk.services.glue.model.FederationSourceErrorCode;

import java.io.IOException;
import java.util.List;

import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.installGoogleCredentialsJsonFile;
import static com.amazonaws.athena.connectors.google.bigquery.BigQueryUtils.setupNativeEnvironmentVariables;

public class BigQueryCompositeHandler
        extends CompositeHandler
{
    private static final Logger logger = LoggerFactory.getLogger(BigQueryCompositeHandler.class);
    //TODO: To be removed, once we expose a way to reuse the BufferAllocator created via the Federation SDK.
    private static final BufferAllocator allocator = new RootAllocator(Long.MAX_VALUE);

    public BigQueryCompositeHandler()
            throws IOException
    {
        super(new BigQueryMetadataHandler(new BigQueryEnvironmentProperties().createEnvironment()), new BigQueryRecordHandler(new BigQueryEnvironmentProperties().createEnvironment(), allocator));
        installGoogleCredentialsJsonFile(new BigQueryEnvironmentProperties().createEnvironment());
        setupNativeEnvironmentVariables();
        logger.info("Inside BigQueryCompositeHandler()");
    }

    @Override
    public Exception handleException(Exception e)
    {
        return handleBigQueryException(super.handleException(e));
    }

    /**
     * Transforms BigQuery exceptions into AthenaConnectorException with appropriate error code.
     * Handles the BigQuery client exceptions (BigQueryException, BigQuerySQLException, JobException) and the
     * gRPC exceptions raised by the BigQuery Storage Read API (ApiException). Only the exception itself is checked,
     * not its causes.
     * Maps BigQuery error codes to FederationSourceErrorCode for better customer experience.
     * Returns other exceptions unchanged, including exceptions that are already AthenaConnectorException.
     *
     * @param e The exception to handle
     * @return The transformed exception or original exception
     */
    public static Exception handleBigQueryException(Exception e)
    {
        if (e instanceof AthenaConnectorException) {
            return e;
        }
        if (!(e instanceof BigQueryException || e instanceof BigQuerySQLException
                || e instanceof JobException || e instanceof ApiException)) {
            return e;
        }
        FederationSourceErrorCode errorCode = mapErrorCode(e);
        logger.info("Mapping BigQuery exception to AthenaConnectorException. Error code: {}, exception type: {}", errorCode, e.getClass().getSimpleName());
        return new AthenaConnectorException(
                e.getMessage() != null ? e.getMessage() : "",
                e,
                ErrorDetails.builder()
                        .errorCode(errorCode.toString())
                        .build()
        );
    }

    private static FederationSourceErrorCode mapErrorCode(Exception bigQueryFailure)
    {
        if (bigQueryFailure instanceof ApiException) {
            // Storage Read API errors. Reuse the HTTP mapping through the HTTP status that gRPC defines for each status code.
            return mapHTTPErrorCode(((ApiException) bigQueryFailure).getStatusCode().getCode().getHttpStatusCode());
        }
        if (bigQueryFailure instanceof BigQueryException) {
            BigQueryException bigQueryException = (BigQueryException) bigQueryFailure;
            FederationSourceErrorCode errorCodeFromReason = mapReason(bigQueryException.getReason());
            return errorCodeFromReason != null ? errorCodeFromReason : mapHTTPErrorCode(bigQueryException.getCode());
        }
        // JobException and BigQuerySQLException do not have an HTTP code, only the BigQuery error reasons.
        List<BigQueryError> errors = bigQueryFailure instanceof JobException
                ? ((JobException) bigQueryFailure).getErrors()
                : ((BigQuerySQLException) bigQueryFailure).getErrors();
        FederationSourceErrorCode errorCodeFromReason = mapReason(firstReason(errors));
        return errorCodeFromReason != null ? errorCodeFromReason : FederationSourceErrorCode.INVALID_INPUT_EXCEPTION;
    }

    private static String firstReason(List<BigQueryError> errors)
    {
        if (errors == null) {
            return null;
        }
        for (BigQueryError error : errors) {
            if (error != null && error.getReason() != null) {
                return error.getReason();
            }
        }
        return null;
    }

    /**
     * Maps the BigQuery error reason to a FederationSourceErrorCode.
     * See https://cloud.google.com/bigquery/docs/error-messages.
     * The reason is more precise than the HTTP code, for example quota errors use HTTP 403.
     *
     * @return The error code, or null when the reason is missing or not mapped.
     */
    private static FederationSourceErrorCode mapReason(String reason)
    {
        if (reason == null) {
            return null;
        }
        switch (reason) {
            case "accessDenied":
            case "blocked":
            case "billingNotEnabled":
                return FederationSourceErrorCode.ACCESS_DENIED_EXCEPTION;
            case "notFound":
                return FederationSourceErrorCode.ENTITY_NOT_FOUND_EXCEPTION;
            case "invalid":
            case "invalidQuery":
            case "duplicate":
                return FederationSourceErrorCode.INVALID_INPUT_EXCEPTION;
            case "rateLimitExceeded":
            case "quotaExceeded":
                return FederationSourceErrorCode.THROTTLING_EXCEPTION;
            case "backendError":
            case "internalError":
            case "jobBackendError":
            case "jobInternalError":
                return FederationSourceErrorCode.INTERNAL_SERVICE_EXCEPTION;
            case "notImplemented":
                return FederationSourceErrorCode.OPERATION_NOT_SUPPORTED_EXCEPTION;
            case "timeout":
                return FederationSourceErrorCode.OPERATION_TIMEOUT_EXCEPTION;
            default:
                return null;
        }
    }
}
