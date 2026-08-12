package com.daop.springai.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import reactor.core.publisher.Flux;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * Stream流式请求
 * </p>
 *
 * @author Daop
 * @since 2026-08-12
 */
@RestController
@RequestMapping("/stream")
public class SE01_StreamController {

    @Value("${spring.ai.dashscope.api-key}")
    private String API_KEY;

    @Value("${ai.chat-url}")
    private String API_URL;

    private static final ExecutorService threadPool = Executors.newVirtualThreadPerTaskExecutor();


    @GetMapping("/outPut")
    public String outPut() throws IOException, InterruptedException {
        String requestBody = """
                {
                    "model": "qwen-plus",
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a helpful assistant."
                        },
                        {
                            "role": "user",
                            "content": "你好，介绍下JAVA？"
                        }
                    ],
                    "stream": true
                }
                """;
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(API_URL))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + API_KEY)
                .header("X-DashScope-SSE", "enable")
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();
        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        System.out.println(response.body());
        return response.body();
    }


    @GetMapping(path = "/outPutStream", produces = MediaType.TEXT_EVENT_STREAM_VALUE + ";charset=UTF-8")
    public SseEmitter outPutStream() {
        SseEmitter emitter = new SseEmitter(60_000L);
        threadPool.submit(() -> {
            String requestBody = """
                    {
                        "model": "qwen-plus",
                        "messages": [
                            {
                                "role": "system",
                                "content": "You are a helpful assistant."
                            },
                            {
                                "role": "user",
                                "content": "你好，介绍下JAVA？"
                            }
                        ],
                        "stream": true
                    }
                    """;
            HttpClient client = HttpClient.newHttpClient();
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(API_URL))
                    .header("Content-Type", "application/json; charset=UTF-8")
                    .header("Authorization", "Bearer " + API_KEY)
                    .header("X-DashScope-SSE", "enable")
                    .POST(HttpRequest.BodyPublishers.ofString(requestBody, StandardCharsets.UTF_8))
                    .build();
            try {
                HttpResponse<InputStream> response = client.send(
                        request,
                        HttpResponse.BodyHandlers.ofInputStream()
                );

                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(response.body(), StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        if (!line.startsWith("data:")) {
                            continue;
                        }

                        String data = line.substring("data:".length()).stripLeading();
                        if ("[DONE]".equals(data)) {
                            break;
                        }

                        System.out.println(data);
                        emitter.send(SseEmitter.event()
                                .data(data, MediaType.APPLICATION_JSON));
                    }
                }
                emitter.complete();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                emitter.completeWithError(e);
            } catch (IOException e) {
                emitter.completeWithError(e);
            }
        });

        return emitter;
    }

    @GetMapping("/sse")
    public SseEmitter sse() {
        SseEmitter emitter = new SseEmitter();

        try (ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor()) {
            executorService.submit(() -> {
                try {
                    for (int i = 0; i < 100; i++) {
                        emitter.send("sseDemo:" + i);
                    }
                } catch (IOException e) {
                    emitter.completeWithError(e);
                } finally {
                    emitter.complete();
                }
            });
        }
        return emitter;
    }

    @GetMapping("/entity")
    public ResponseEntity<StreamingResponseBody> chat() {
        StreamingResponseBody output = outputStream -> {
            for (int i = 0; i < 100; i++) {
                String data = "sseDemo:" + i + "\n\n";
                outputStream.write(data.getBytes(StandardCharsets.UTF_8));
                outputStream.flush();
            }
        };

        return ResponseEntity.ok()
                .header("Content-Type", MediaType.TEXT_EVENT_STREAM_VALUE)
                .body(output);
    }

    @GetMapping("/flux")
    public Flux<String> flux() {
        return Flux.range(1, 100)
                .map(i -> "sseDemo:" + i + "\n\n");
    }
}
