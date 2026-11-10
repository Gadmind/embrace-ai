package com.daop.springai.controller;

import org.springframework.ai.chat.client.ChatClient;
import org.springframework.ai.chat.model.ChatModel;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 * 聊天客户端控制器
 * </p>
 *
 * @author Daop
 * @since 2026-08-17
 */
@RestController
@RequestMapping("/client")
public class SE03_ChatClientController implements InitializingBean {
    @Autowired
    private ChatModel chatModel;

    private ChatClient chatClient;

    @GetMapping("/call")
    public String call(String message) {
        return chatClient.prompt(message).call().content();
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        System.out.println("初始化聊天客户端1111");
        chatClient = ChatClient
                .builder(chatModel)
                .build();
    }
}
