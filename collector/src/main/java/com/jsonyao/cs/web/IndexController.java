package com.jsonyao.cs.web;

import com.jsonyao.cs.util.InputMDC;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class IndexController {

	/**
	 * 日志格式解释:
	 * [%d{yyyy-MM-dd'T'HH:mm:ss.SSSZZ}]: 东八区日期
	 * [%level{length=5}]: 日志级别
	 * [%thread-%tid]: 当前线程ID
	 * [%logger]: 当前类即logger
	 * [%X{hostName}]: 自定义变量, 主机名称
	 * [%X{ip}]: 自定义变量, IP
	 * [%X{applicationName}]: 自定义变量, 应用名称
	 * [%F,%L,%C,%M]: 当前执行的文件, 当前行号, 当前类的全类名, 当前方法名
	 * [%m] ## '%ex'%n: message、异常、换行
	 * @return
	 */
	@RequestMapping(value = "/index")
	public String index() {
		// 设置MDC线程局部变量
		InputMDC.putMDC();

		log.info("我是一条info日志");
		log.warn("我是一条warn日志");
		log.error("我是一条error日志");
		return "idx";
	}

	@RequestMapping(value = "/err")
	public String err(){
		// 设置MDC线程局部变量
		InputMDC.putMDC();

		try {
			int a = 1 / 0;
		} catch (Exception e){
			e.printStackTrace();
			log.error("算术异常", e);
		}

		return "err";
	}
}
