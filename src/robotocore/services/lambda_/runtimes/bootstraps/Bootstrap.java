import java.io.*;
import java.lang.reflect.*;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.stream.*;

/**
 * Java Lambda bootstrap — reads event from stdin, invokes handler, writes result to stdout.
 *
 * Usage: java -cp .:* Bootstrap
 * Handler is read from _HANDLER env var (e.g., "com.example.Handler::handleRequest")
 *
 * Protocol:
 *   stdin  -> JSON event (as String — handler receives raw string)
 *   stdout -> JSON result (handler return value toString)
 *   stderr -> logs
 *
 * This is a minimal bootstrap. For full Java Lambda support, the handler receives
 * the event as a Map or InputStream. This bootstrap passes the raw JSON string
 * and converts the result to JSON string output.
 */
public class Bootstrap {
    public static void main(String[] args) {
        String handlerSpec = System.getenv("_HANDLER");
        if (handlerSpec == null && args.length > 0) handlerSpec = args[0];
        if (handlerSpec == null) {
            System.out.println("{\"errorMessage\":\"No handler specified\",\"errorType\":\"Runtime.HandlerNotFound\"}");
            System.exit(1);
        }

        // Parse handler: "package.Class::method" or "package.Class"
        String className;
        String methodName = "handleRequest";
        if (handlerSpec.contains("::")) {
            String[] parts = handlerSpec.split("::");
            className = parts[0];
            methodName = parts[1];
        } else {
            className = handlerSpec;
        }

        // Read event from stdin
        String eventJson;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(System.in))) {
            eventJson = reader.lines().collect(Collectors.joining("\n"));
        } catch (IOException e) {
            eventJson = "{}";
        }

        // Load the handler class
        Class<?> handlerClass;
        try {
            // Add current directory and all JARs to classpath
            handlerClass = Class.forName(className);
        } catch (ClassNotFoundException e) {
            System.out.printf("{\"errorMessage\":\"Cannot find class '%s': %s\",\"errorType\":\"Runtime.ImportModuleError\"}%n",
                className, e.getMessage());
            System.exit(1);
            return;
        }

        // Create instance
        Object instance;
        try {
            instance = handlerClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            System.out.printf("{\"errorMessage\":\"Cannot instantiate '%s': %s\",\"errorType\":\"Runtime.ImportModuleError\"}%n",
                className, e.getMessage());
            System.exit(1);
            return;
        }

        // Find the handler method — try common signatures
        Method method = null;
        try {
            // Try (String event, Object context) -> String
            method = handlerClass.getMethod(methodName, String.class, Object.class);
        } catch (NoSuchMethodException e1) {
            try {
                // Try (Map event, Object context) -> Object
                method = handlerClass.getMethod(methodName, Map.class, Object.class);
            } catch (NoSuchMethodException e2) {
                try {
                    // Try (InputStream, OutputStream, Object) -> void
                    method = handlerClass.getMethod(methodName, InputStream.class, OutputStream.class, Object.class);
                } catch (NoSuchMethodException e3) {
                    try {
                        // Try (Object event, Object context) -> Object
                        method = handlerClass.getMethod(methodName, Object.class, Object.class);
                    } catch (NoSuchMethodException e4) {
                        System.out.printf(
                            "{\"errorMessage\":\"Handler method '%s' not found in '%s'\",\"errorType\":\"Runtime.HandlerNotFound\"}%n",
                            methodName, className);
                        System.exit(1);
                        return;
                    }
                }
            }
        }

        // Build a simple context map
        Map<String, Object> context = new HashMap<>();
        context.put("functionName", System.getenv().getOrDefault("AWS_LAMBDA_FUNCTION_NAME", "test-function"));
        context.put("functionVersion", "$LATEST");
        context.put("memoryLimitInMB", Integer.parseInt(System.getenv().getOrDefault("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "128")));
        context.put("awsRequestId", UUID.randomUUID().toString());

        // Invoke
        try {
            Object result;
            Class<?>[] paramTypes = method.getParameterTypes();

            if (paramTypes.length == 3 && paramTypes[0] == InputStream.class) {
                // Stream handler: (InputStream, OutputStream, Context)
                ByteArrayInputStream in = new ByteArrayInputStream(eventJson.getBytes());
                ByteArrayOutputStream out = new ByteArrayOutputStream();
                method.invoke(instance, in, out, context);
                System.out.print(out.toString());
            } else if (paramTypes[0] == String.class) {
                result = method.invoke(instance, eventJson, context);
                System.out.print(result == null ? "null" : result.toString());
            } else if (paramTypes[0] == Map.class) {
                // Simple JSON-to-Map conversion (no external deps)
                // Pass the raw string wrapped in a map for simplicity
                Map<String, Object> eventMap = new HashMap<>();
                eventMap.put("_raw", eventJson);
                result = method.invoke(instance, eventMap, context);
                System.out.print(result == null ? "null" : result.toString());
            } else {
                result = method.invoke(instance, eventJson, context);
                System.out.print(result == null ? "null" : result.toString());
            }
        } catch (InvocationTargetException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            StringWriter sw = new StringWriter();
            cause.printStackTrace(new PrintWriter(sw));
            System.err.print(sw.toString());
            System.out.printf(
                "{\"errorMessage\":\"%s\",\"errorType\":\"%s\"}",
                cause.getMessage() != null ? cause.getMessage().replace("\"", "\\\"") : "Unknown error",
                cause.getClass().getSimpleName());
            System.exit(1);
        } catch (Exception e) {
            System.err.println(e.toString());
            System.out.printf(
                "{\"errorMessage\":\"%s\",\"errorType\":\"%s\"}",
                e.getMessage() != null ? e.getMessage().replace("\"", "\\\"") : "Unknown error",
                e.getClass().getSimpleName());
            System.exit(1);
        }
    }
}
