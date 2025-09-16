package com.sixgroup.referencedata.infrastructure.exception;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.client.HttpServerErrorException.InternalServerError;

import com.sixgroup.referencedata.domain.exception.EnrichedTradeNotFoundException;
import com.sixgroup.referencedata.domain.exception.IsinNotFoundException;
import com.sixgroup.referencedata.infrastructure.controller.model.ErrorResponseRDTO;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<ErrorResponseRDTO> handleBadRequest(Exception e) {
        return this.buildResponse(HttpStatus.BAD_REQUEST, e);
    }

    @ExceptionHandler({IsinNotFoundException.class, EnrichedTradeNotFoundException.class})
    public ResponseEntity<ErrorResponseRDTO> handleNotFound(Exception e) {
        return this.buildResponse(HttpStatus.NOT_FOUND, e);
    }

    @ExceptionHandler({InternalServerError.class})
    public ResponseEntity<ErrorResponseRDTO> handleInternalServerException(Exception ex) {
        return this.buildResponse(HttpStatus.INTERNAL_SERVER_ERROR, ex);
    }

    private ResponseEntity<ErrorResponseRDTO> buildResponse(HttpStatus httpStatus, Exception e) {
        ErrorResponseRDTO errorResponseRDTO = new ErrorResponseRDTO()
            .message(e.getMessage())
            .type(httpStatus.getReasonPhrase())
            .code(httpStatus.value());

        return new ResponseEntity<>(errorResponseRDTO, httpStatus);
    }

}
