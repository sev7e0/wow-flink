package com.sev7e0.wow.java.utils;

import java.util.HashMap;
import java.util.Map;

public class Message{

	public static Message produceMessage(){
		Map<String, Object> hashMap = new HashMap<>();
		hashMap.put("type", "custom");
		hashMap.put("status", "received");
		hashMap.put("delay", false);
		return new Message("sev7e0", "mario", System.currentTimeMillis(), "super mario Odyssey", hashMap);
	}

	public Message() {
	}

	public Message(String sender, String receiver, Long time, String content, Map<String, Object> tags) {
		this.sender = sender;
		this.receiver = receiver;
		this.time = time;
		this.content = content;
		this.tags = tags;
	}

	private String sender;
	private String receiver;
	private Long time;
	private String content;
	private Map<String,Object> tags;

	public String getSender() {
		return sender;
	}

	public void setSender(String sender) {
		this.sender = sender;
	}

	public String getReceiver() {
		return receiver;
	}

	public void setReceiver(String receiver) {
		this.receiver = receiver;
	}

	public Long getTime() {
		return time;
	}

	public void setTime(Long time) {
		this.time = time;
	}

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public Map<String, Object> getTags() {
		return tags;
	}

	public void setTags(Map<String, Object> tags) {
		this.tags = tags;
	}

	@Override
	public String toString() {
		return "Message{" +
			"sender='" + sender + '\'' +
			", receiver='" + receiver + '\'' +
			", time=" + time +
			", content='" + content + '\'' +
			", tags=" + tags +
			'}';
	}
}
