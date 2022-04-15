package net.bitnine.agenspop.exception;

import org.springframework.boot.web.servlet.error.ErrorController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class CustomErrorController implements ErrorController {

    private static final String PATH = "/error";

    @RequestMapping(value = PATH)
    public String error() {
        return "{ \"msg\": \"customError\"}";
    }

    @Override
    public String getErrorPath() {
        return PATH;
    }
}