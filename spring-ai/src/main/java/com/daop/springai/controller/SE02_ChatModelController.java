package com.daop.springai.controller;

import com.alibaba.cloud.ai.dashscope.chat.DashScopeChatModel;
import jakarta.servlet.http.HttpServletResponse;
import org.springframework.ai.chat.messages.SystemMessage;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.model.ChatResponse;
import org.springframework.ai.chat.prompt.ChatOptions;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

/**
 * <p>
 * 调用控制器
 * </p>
 *
 * @author Daop
 * @since 2026-08-17
 */
@RestController
@RequestMapping("/chat")
public class SE02_ChatModelController {

    @Autowired
    private DashScopeChatModel dashScopeChatModel;

    @GetMapping("/call/string")
    public String callString(String message) {
        return dashScopeChatModel.call(message);
    }

    @GetMapping("/call/message")
    public String callMessage(String message) {
        SystemMessage systemMessage = new SystemMessage("你是一个翻译工具，请将用户输入的内容翻译成英文");
        UserMessage userMessage = new UserMessage(message);
        return dashScopeChatModel.call(systemMessage, userMessage);
    }

    @GetMapping("/call/prompt")
    public String callPrompt(String message) {
        SystemMessage systemMessage = new SystemMessage("你是一个有用的工具，请先和我说尊敬的主人后，再开始回答问题");
        UserMessage userMessage = new UserMessage(message);
        ChatOptions chatOptions = ChatOptions
                .builder()
                .model("deepseek-r1")
                .build();
        Prompt prompt = new Prompt.Builder()
                .messages(systemMessage, userMessage)
                .chatOptions(chatOptions)
                .build();
        ChatResponse chatResponse = dashScopeChatModel.call(prompt);
        return chatResponse.getResult().getOutput().getText();
    }

    @GetMapping("/stream/string")
    public Flux<String> streamString(String message, HttpServletResponse response) {
        response.setCharacterEncoding("UTF-8");
        return dashScopeChatModel.stream(message);
    }

    @GetMapping("/stream/message")
    public Flux<String> streamMessage(String message, HttpServletResponse response) {
        response.setCharacterEncoding("UTF-8");

        SystemMessage systemMessage = new SystemMessage("你是一个翻译工具，请将用户输入的内容翻译成英文");
        UserMessage userMessage = new UserMessage(message);
        return dashScopeChatModel.stream(systemMessage, userMessage);
    }

    @GetMapping("/stream/prompt")
    public Flux<String> streamPrompt(String message, HttpServletResponse response) {
        response.setCharacterEncoding("UTF-8");

        SystemMessage systemMessage = new SystemMessage("你是一个有用的工具，请先和我说尊敬的主人后，再开始回答问题");
        UserMessage userMessage = new UserMessage(message);
        ChatOptions chatOptions = ChatOptions
                .builder()
                .model("deepseek-r1")
                .build();
        Prompt prompt = new Prompt.Builder()
                .messages(systemMessage, userMessage)
                .chatOptions(chatOptions)
                .build();
        return dashScopeChatModel.stream(prompt).mapNotNull(chatResponse -> chatResponse.getResult().getOutput().getText());
    }
}
