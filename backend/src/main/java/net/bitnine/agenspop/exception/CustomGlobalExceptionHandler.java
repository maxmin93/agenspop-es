package net.bitnine.agenspop.exception;

import net.bitnine.agenspop.graph.exception.AgensGraphException;
import net.bitnine.agenspop.graph.exception.AgensGraphManagerException;
import net.bitnine.agenspop.service.AgensGremlinException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.WebRequest;
// import org.springframework.web.servlet.mvc.method.annotation.ResponseEntityExceptionHandler;

import java.time.LocalDateTime;

@ControllerAdvice
@RestController
public class CustomGlobalExceptionHandler /* extends ResponseEntityExceptionHandler */ {

    @ExceptionHandler(AgensGremlinException.class)
    public ResponseEntity<CustomErrorResponse> handleAgensWebServiceException(
            Exception ex, WebRequest request) {
        System.out.println("CustomGlobalExceptionHandler ==> "+ex);

        CustomErrorResponse errors = new CustomErrorResponse();
        errors.setStatus(HttpStatus.NOT_FOUND.value());
        errors.setTimestamp(LocalDateTime.now());
        errors.setType("AgensGremlinException");
        errors.setError(ex.getMessage());
        if( ex.getCause() != null ) errors.setCause(ex.getCause().getMessage());

        return new ResponseEntity<>(errors, HttpStatus.BAD_REQUEST);
    }

    @ExceptionHandler(AgensGraphManagerException.class)
    public ResponseEntity<CustomErrorResponse> handleAgensGraphManagerException(
            Exception ex, WebRequest request) {
        System.out.println("CustomGlobalExceptionHandler ==> "+ex);

        CustomErrorResponse errors = new CustomErrorResponse();
        errors.setStatus(HttpStatus.NOT_FOUND.value());
        errors.setTimestamp(LocalDateTime.now());
        errors.setType("AgensGraphManagerException");
        errors.setError(ex.getMessage());
        if( ex.getCause() != null ) errors.setCause(ex.getCause().getMessage());

        return new ResponseEntity<>(errors, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(AgensGraphException.class)
    public ResponseEntity<CustomErrorResponse> handleAgensGraphException(
            Exception ex, WebRequest request) {
        System.out.println("CustomGlobalExceptionHandler ==> "+ex);

        CustomErrorResponse errors = new CustomErrorResponse();
        errors.setStatus(HttpStatus.NOT_FOUND.value());
        errors.setTimestamp(LocalDateTime.now());
        errors.setType("AgensGraphException");
        errors.setError(ex.getMessage());
        if( ex.getCause() != null ) errors.setCause(ex.getCause().getMessage());

        return new ResponseEntity<>(errors, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<CustomErrorResponse> handleIllegalArgumentException(
            Exception ex, WebRequest request) {
        System.out.println("CustomGlobalExceptionHandler ==> "+ex);

        CustomErrorResponse errors = new CustomErrorResponse();
        errors.setStatus(HttpStatus.NOT_FOUND.value());
        errors.setTimestamp(LocalDateTime.now());
        errors.setType("IllegalArgumentException");
        errors.setError(ex.getMessage());
        if( ex.getCause() != null ) errors.setCause(ex.getCause().getMessage());

        return new ResponseEntity<>(errors, HttpStatus.NOT_FOUND);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<CustomErrorResponse> handleDefaultException(
            Exception ex, WebRequest request) {
        System.out.print("CustomGlobalExceptionHandler ==> "+ex.toString());
        // ex.printStackTrace();

        CustomErrorResponse errors = new CustomErrorResponse();
        errors.setStatus(HttpStatus.BAD_REQUEST.value());
        errors.setTimestamp(LocalDateTime.now());
        errors.setType("OtherException");
        errors.setError(ex.toString());
        if( ex.getCause() != null ) errors.setCause(ex.getCause().getMessage());
        else errors.setCause("RuntimeException(*)");

        return new ResponseEntity<>(errors, HttpStatus.BAD_REQUEST);
    }

    //...
}