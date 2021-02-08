package com.jsonyao.cs.web;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class IndexController {

	/**
	 * [%d{yyyy-MM-dd'T'HH:mm:ss.SSSZZ}]: 日期
	 * [%level{length=5}]: 级别
	 * [%thread-%tid]: 线程ID
	 * [%logger]: logger
	 * [%X{hostName}]: 自定义配置, 主机名称
	 * [%X{ip}]: 自定义配置, IP
	 * [%X{applicationName}]: 自定义配置, 应用名称
	 * [%F,%L,%C,%M]: 方法, 行号, 列好, 方法名
	 * [%m] ## '%ex'%n: message、异常、换行
	 * @return
	 */
	@RequestMapping(value = "/index")
	public String index() {
		log.info("我是一条info日志");
		log.warn("我是一条warn日志");
		log.error("我是一条error日志");
		return "idx";
	}

}
