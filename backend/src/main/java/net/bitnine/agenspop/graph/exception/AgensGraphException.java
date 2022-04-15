package net.bitnine.agenspop.graph.exception;

public class AgensGraphException extends RuntimeException {

    private String message;
    private String details;
    private String hint;
    private String nextActions;
    private String support;

    public AgensGraphException(String message) {
        super(message);
    }

    public AgensGraphException(String message, String details, String hint, String nextActions, String support) {
        this.message = message;
        this.details = details;
        this.hint = hint;
        this.nextActions = nextActions;
        this.support = support;
    }
}