package com.jsonyao.cs.entity;

import lombok.Data;

/**
 * 测试Watcher报警实体
 */
@Data
public class AccurateWatcherMessage {

	/**
	 * 异常错误告警Title
	 */
	private String title;

	/**
	 * 执行时间
	 */
	private String executionTime;

	/**
	 * 应用名称
	 */
	private String applicationName;

	/**
	 * 日志等级
	 */
	private String level;

	/**
	 * 异常错误告警内容
	 */
	private String body;
}
