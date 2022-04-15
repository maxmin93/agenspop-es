package net.bitnine.agenspop.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import net.bitnine.agenspop.config.properties.ProductProperties;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
// import reactor.core.publisher.Flux;

import java.util.stream.Collectors;
import java.util.stream.Stream;

public final class AgensUtilHelper {

    public static final HttpHeaders productHeaders(ProductProperties productProperties){
        HttpHeaders headers = new HttpHeaders();
        headers.add("agens.product.name", productProperties.getName());
        headers.add("agens.product.version", productProperties.getVersion());
        return headers;
    }
/*
    public static <T> ResponseEntity<Flux<String>> responseStream(
            ObjectMapper mapper, HttpHeaders headers, Stream<T> stream
    ) {
        // **NOTE: cannot use StringJoiner beacause it's Streaming and Map function
        // https://stackoverflow.com/a/50988970/6811653
        // StringJoiner sj = new StringJoiner(",");
        Stream<String> vstream = stream.map(r ->
                wrapException(() -> mapper.writeValueAsString(r) +"\n" )
        );
        return new ResponseEntity( Flux.fromStream(vstream), headers, HttpStatus.OK);
    }
*/

    public static <T> ResponseEntity<String> responseStream(
            ObjectMapper mapper, HttpHeaders headers, Stream<T> stream
    ) {
        // **NOTE: cannot use StringJoiner beacause it's Streaming and Map function
        // https://stackoverflow.com/a/50988970/6811653
        // StringJoiner sj = new StringJoiner(",");
        Stream<String> vstream = stream.map(r ->
                wrapException(() -> mapper.writeValueAsString(r))
        );
        return new ResponseEntity( vstream.collect(Collectors.joining(",", "[", "]"))
                , headers, HttpStatus.OK);
    }

    // **NOTE: Java Exception Handle in Stream Operations
    // https://kihoonkim.github.io/2017/09/09/java/noexception-in-stream-operations/

    public interface ExceptionSupplier<T> {
        T get() throws Exception;
    }

    public static <T> T wrapException(ExceptionSupplier<T> z) {
        try {
            return z.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
